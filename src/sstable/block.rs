//! Data / index block format (same idea as LevelDB):
//!
//! ```text
//! entry*  restart_offset: u32 * R  R: u32
//! entry = shared: varint | unshared: varint | value_len: varint
//!         | key_delta: [u8; unshared] | value: [u8; value_len]
//! ```
//!
//! Keys are prefix-compressed against the previous key. Every 16th entry
//! (a "restart point") stores its full key, so a seek can binary search the
//! restart points and then scan at most 16 entries.

use std::cmp::Ordering;

use bytes::Bytes;

use crate::coding::{get_u32, get_varint, put_u32, put_varint};
use crate::error::{Error, Result};
use crate::key;

pub const RESTART_INTERVAL: usize = 16;

#[derive(Debug)]
pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    last_key: Vec<u8>,
    counter: usize,
}

impl Default for BlockBuilder {
    fn default() -> Self {
        BlockBuilder { buf: Vec::with_capacity(4096 + 512), restarts: vec![0], last_key: Vec::new(), counter: 0 }
    }
}

impl BlockBuilder {
    /// Keys must arrive in sorted internal-key order.
    pub fn add(&mut self, ikey: &[u8], value: &[u8]) {
        debug_assert!(self.is_empty() || key::compare(&self.last_key, ikey) == Ordering::Less);
        let shared = if self.counter < RESTART_INTERVAL {
            self.last_key.iter().zip(ikey).take_while(|(a, b)| a == b).count()
        } else {
            self.restarts.push(self.buf.len() as u32);
            self.counter = 0;
            0
        };
        put_varint(&mut self.buf, shared as u64);
        put_varint(&mut self.buf, (ikey.len() - shared) as u64);
        put_varint(&mut self.buf, value.len() as u64);
        self.buf.extend_from_slice(&ikey[shared..]);
        self.buf.extend_from_slice(value);
        self.last_key.clear();
        self.last_key.extend_from_slice(ikey);
        self.counter += 1;
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Size of the block if finished now.
    pub fn estimated_size(&self) -> usize {
        self.buf.len() + self.restarts.len() * 4 + 4
    }

    pub fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    /// Appends the finished block to `out` and resets for reuse. The
    /// builder keeps its buffers, so a long flush doesn't re-allocate per block.
    pub fn finish_into(&mut self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.buf);
        for &r in &self.restarts {
            put_u32(out, r);
        }
        put_u32(out, self.restarts.len() as u32);
        self.buf.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.last_key.clear();
        self.counter = 0;
    }
}

/// A decoded block. Holds `Bytes`, so cloning it (e.g. out of the cache) is
/// a refcount bump, not a copy.
#[derive(Debug, Clone)]
pub struct Block {
    data: Bytes,
    restarts_off: usize,
    num_restarts: usize,
}

impl Block {
    pub fn new(data: Bytes) -> Result<Block> {
        let len = data.len();
        let n = get_u32(&data, len.checked_sub(4).ok_or_else(|| Error::corruption("block too short"))?)
            .ok_or_else(|| Error::corruption("block too short"))? as usize;
        let restarts_bytes = n.checked_mul(4).ok_or_else(|| Error::corruption("bad restart count"))?;
        let restarts_off = len
            .checked_sub(4)
            .and_then(|l| l.checked_sub(restarts_bytes))
            .ok_or_else(|| Error::corruption("bad restart count"))?;
        if n == 0 {
            return Err(Error::corruption("block has no restart points"));
        }
        Ok(Block { data, restarts_off, num_restarts: n })
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    fn restart(&self, i: usize) -> Result<usize> {
        let off = get_u32(&self.data, self.restarts_off + i * 4)
            .ok_or_else(|| Error::corruption("restart out of range"))? as usize;
        if off > self.restarts_off {
            return Err(Error::corruption("restart offset past entries"));
        }
        Ok(off)
    }

    pub fn iter(&self) -> BlockIter {
        BlockIter { block: self.clone(), key: Vec::new(), val: (0, 0), next: 0, valid: false }
    }
}

/// Cursor over a block. Owns its block (a `Bytes` clone), so it has no
/// lifetime and can live inside a table or merge iterator.
#[derive(Debug)]
pub struct BlockIter {
    block: Block,
    /// Current full key, rebuilt from prefix-compressed entries. Reused.
    key: Vec<u8>,
    /// Current value as a range into `block.data`.
    val: (usize, usize),
    /// Offset of the next entry.
    next: usize,
    valid: bool,
}

impl BlockIter {
    pub fn valid(&self) -> bool {
        self.valid
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.block.data[self.val.0..self.val.1]
    }

    /// The value as `Bytes` sharing the block's buffer (no copy).
    pub fn value_bytes(&self) -> Bytes {
        self.block.data.slice(self.val.0..self.val.1)
    }

    /// Decodes the entry at `self.next`. Sets `valid = false` at the end.
    fn parse_next(&mut self) -> Result<()> {
        let end = self.block.restarts_off;
        if self.next >= end {
            self.valid = false;
            return Ok(());
        }
        let d = &self.block.data[self.next..end];
        let bad = || Error::corruption("bad block entry");
        let (shared, a) = get_varint(d).ok_or_else(bad)?;
        let (unshared, b) = get_varint(&d[a..]).ok_or_else(bad)?;
        let (vlen, c) = get_varint(&d[a + b..]).ok_or_else(bad)?;
        let (shared, unshared, vlen) = (shared as usize, unshared as usize, vlen as usize);
        let hdr = a + b + c;
        let key_end = hdr.checked_add(unshared).ok_or_else(bad)?;
        let val_end = key_end.checked_add(vlen).ok_or_else(bad)?;
        if val_end > d.len() || shared > self.key.len() {
            return Err(bad());
        }
        self.key.truncate(shared);
        self.key.extend_from_slice(&d[hdr..key_end]);
        self.val = (self.next + key_end, self.next + val_end);
        self.next += val_end;
        self.valid = true;
        Ok(())
    }

    pub fn seek_to_first(&mut self) -> Result<()> {
        self.key.clear();
        self.next = 0;
        self.parse_next()
    }

    pub fn next(&mut self) -> Result<()> {
        self.parse_next()
    }

    fn seek_to_restart(&mut self, i: usize) -> Result<()> {
        self.key.clear();
        self.next = self.block.restart(i)?;
        self.parse_next()
    }

    /// Moves to the first entry with key >= `target` (internal-key order).
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        // Binary search: last restart point whose key is < target.
        let (mut lo, mut hi) = (0usize, self.block.num_restarts - 1);
        while lo < hi {
            let mid = (lo + hi).div_ceil(2);
            self.seek_to_restart(mid)?;
            if !self.valid {
                return Err(Error::corruption("restart point has no entry"));
            }
            if key::compare(&self.key, target) == Ordering::Less {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        self.seek_to_restart(lo)?;
        while self.valid && key::compare(&self.key, target) == Ordering::Less {
            self.parse_next()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{encode, ValueKind};

    fn build(n: usize) -> Block {
        let mut b = BlockBuilder::default();
        for i in 0..n {
            b.add(&encode(format!("key{i:05}").as_bytes(), 1, ValueKind::Put), format!("v{i}").as_bytes());
        }
        let mut out = Vec::new();
        b.finish_into(&mut out);
        Block::new(out.into()).unwrap()
    }

    #[test]
    fn iterate_all() {
        let blk = build(100);
        let mut it = blk.iter();
        it.seek_to_first().unwrap();
        let mut n = 0;
        while it.valid() {
            assert_eq!(key::user_key(it.key()), format!("key{n:05}").as_bytes());
            assert_eq!(it.value(), format!("v{n}").as_bytes());
            it.next().unwrap();
            n += 1;
        }
        assert_eq!(n, 100);
    }

    #[test]
    fn seek_every_key_and_gaps() {
        let blk = build(100);
        let mut it = blk.iter();
        for i in 0..100 {
            it.seek(&key::seek_key(format!("key{i:05}").as_bytes(), 10)).unwrap();
            assert!(it.valid());
            assert_eq!(key::user_key(it.key()), format!("key{i:05}").as_bytes());
        }
        it.seek(&key::seek_key(b"key00050x", 10)).unwrap();
        assert_eq!(key::user_key(it.key()), b"key00051");
        it.seek(&key::seek_key(b"zzz", 10)).unwrap();
        assert!(!it.valid());
        it.seek(&key::seek_key(b"", 10)).unwrap();
        assert_eq!(key::user_key(it.key()), b"key00000");
    }

    #[test]
    fn garbage_is_error_not_panic() {
        for len in 0..64 {
            let junk: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
            if let Ok(b) = Block::new(junk.into()) {
                let mut it = b.iter();
                let _ = it.seek_to_first();
                let _ = it.seek(b"abcdefghijk");
            }
        }
    }
}
