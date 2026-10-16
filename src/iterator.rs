//! Iterators over internal keys, and the merge that turns many sorted
//! sources (memtables + SSTables) into one sorted stream.

use std::cmp::Ordering;

use bytes::Bytes;

use crate::error::Result;
use crate::key::{self, ValueKind};

/// A sorted cursor over internal keys. Blocking: SST-backed iterators may
/// `pread`, so these only run on our threads or inside `spawn_blocking`.
pub trait InternalIterator: Send {
    fn valid(&self) -> bool;
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
    /// Value as shared `Bytes`: zero-copy for SST blocks and memtables.
    fn value_bytes(&self) -> Bytes;
    fn next(&mut self) -> Result<()>;
    /// First entry with key >= `target`.
    fn seek(&mut self, target: &[u8]) -> Result<()>;
    fn seek_to_first(&mut self) -> Result<()>;
}

/// Merges N sorted children into one sorted stream.
///
/// Finds the smallest child with a linear scan, not a heap. N is small here
/// (a few memtables + L0 files + one iterator per deeper level, so ~10-20),
/// and for N that small a linear scan over hot cache lines beats a heap's
/// pointer chasing. LevelDB's `MergingIterator` makes the same choice.
pub struct MergingIterator {
    children: Vec<Box<dyn InternalIterator>>,
    current: Option<usize>,
}

impl std::fmt::Debug for MergingIterator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergingIterator").field("children", &self.children.len()).finish()
    }
}

impl MergingIterator {
    /// Children must be ordered newest source first. On equal keys (which
    /// can't happen with unique seqs, but just in case) the newer one wins.
    pub fn new(children: Vec<Box<dyn InternalIterator>>) -> Self {
        MergingIterator { children, current: None }
    }

    fn find_smallest(&mut self) {
        let mut best: Option<usize> = None;
        for (i, c) in self.children.iter().enumerate() {
            if !c.valid() {
                continue;
            }
            best = match best {
                None => Some(i),
                Some(b) if key::compare(c.key(), self.children[b].key()) == Ordering::Less => Some(i),
                keep => keep,
            };
        }
        self.current = best;
    }
}

impl InternalIterator for MergingIterator {
    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn key(&self) -> &[u8] {
        self.current.map_or(&[], |i| self.children[i].key())
    }

    fn value(&self) -> &[u8] {
        self.current.map_or(&[], |i| self.children[i].value())
    }

    fn value_bytes(&self) -> Bytes {
        self.current.map_or_else(Bytes::new, |i| self.children[i].value_bytes())
    }

    fn next(&mut self) -> Result<()> {
        if let Some(i) = self.current {
            self.children[i].next()?;
        }
        self.find_smallest();
        Ok(())
    }

    fn seek(&mut self, target: &[u8]) -> Result<()> {
        for c in &mut self.children {
            c.seek(target)?;
        }
        self.find_smallest();
        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<()> {
        for c in &mut self.children {
            c.seek_to_first()?;
        }
        self.find_smallest();
        Ok(())
    }
}

/// Turns the internal stream into what a user sees at snapshot `seq`:
/// one entry per user key (its newest visible version), tombstones hidden,
/// versions newer than `seq` skipped.
pub struct UserIterator<I: InternalIterator> {
    inner: I,
    seq: u64,
    /// Current user key and value. Owned copies: the inner iterator moves on.
    key: Vec<u8>,
    value: Bytes,
    valid: bool,
    skip: Vec<u8>,
    has_skip: bool,
}

impl<I: InternalIterator> std::fmt::Debug for UserIterator<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserIterator").field("seq", &self.seq).field("valid", &self.valid).finish()
    }
}

impl<I: InternalIterator> UserIterator<I> {
    pub fn new(inner: I, seq: u64) -> Self {
        UserIterator {
            inner,
            seq,
            key: Vec::new(),
            value: Bytes::new(),
            valid: false,
            skip: Vec::new(),
            has_skip: false,
        }
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &Bytes {
        &self.value
    }

    pub fn seek(&mut self, user_key: &[u8]) -> Result<()> {
        self.inner.seek(&key::seek_key(user_key, self.seq))?;
        self.has_skip = false;
        self.settle()
    }

    pub fn seek_to_first(&mut self) -> Result<()> {
        self.inner.seek_to_first()?;
        self.has_skip = false;
        self.settle()
    }

    pub fn next(&mut self) -> Result<()> {
        if !self.valid {
            return Ok(());
        }
        // Skip every older version of the key we just returned. Swap the
        // buffers instead of copying: no allocation per step.
        std::mem::swap(&mut self.key, &mut self.skip);
        self.has_skip = true;
        self.settle()
    }

    /// Advances `inner` to the next user key that has a visible, live value.
    fn settle(&mut self) -> Result<()> {
        while self.inner.valid() {
            let ik = self.inner.key();
            let Some((seq, kind)) = key::seq_kind(ik) else {
                return Err(crate::Error::corruption("bad internal key"));
            };
            let uk = key::user_key(ik);
            // `skip` holds a user key whose newest visible version we've
            // already handled (returned, or found to be a tombstone).
            if seq > self.seq || (self.has_skip && self.skip.as_slice() == uk) {
                self.inner.next()?;
                continue;
            }
            match kind {
                ValueKind::Delete => {
                    self.skip.clear();
                    self.skip.extend_from_slice(uk);
                    self.has_skip = true;
                    self.inner.next()?;
                }
                ValueKind::Put => {
                    self.key.clear();
                    self.key.extend_from_slice(uk);
                    self.value = self.inner.value_bytes();
                    self.valid = true;
                    return Ok(());
                }
            }
        }
        self.valid = false;
        Ok(())
    }
}

/// A sorted in-memory iterator, for tests.
#[cfg(test)]
pub struct VecIter {
    items: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
}

#[cfg(test)]
impl VecIter {
    pub fn new(mut items: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        items.sort_by(|a, b| key::compare(&a.0, &b.0));
        VecIter { items, pos: usize::MAX }
    }
}

#[cfg(test)]
impl InternalIterator for VecIter {
    fn valid(&self) -> bool {
        self.pos < self.items.len()
    }
    fn key(&self) -> &[u8] {
        &self.items[self.pos].0
    }
    fn value(&self) -> &[u8] {
        &self.items[self.pos].1
    }
    fn value_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.items[self.pos].1)
    }
    fn next(&mut self) -> Result<()> {
        self.pos += 1;
        Ok(())
    }
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.pos = self.items.partition_point(|(k, _)| key::compare(k, target) == Ordering::Less);
        Ok(())
    }
    fn seek_to_first(&mut self) -> Result<()> {
        self.pos = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::encode;

    fn put(k: &str, seq: u64, v: &str) -> (Vec<u8>, Vec<u8>) {
        (encode(k.as_bytes(), seq, ValueKind::Put), v.as_bytes().to_vec())
    }
    fn del(k: &str, seq: u64) -> (Vec<u8>, Vec<u8>) {
        (encode(k.as_bytes(), seq, ValueKind::Delete), Vec::new())
    }

    fn collect(mut it: UserIterator<MergingIterator>) -> Vec<(String, String)> {
        let mut out = Vec::new();
        it.seek_to_first().unwrap();
        while it.valid() {
            out.push((String::from_utf8(it.key().to_vec()).unwrap(), String::from_utf8(it.value().to_vec()).unwrap()));
            it.next().unwrap();
        }
        out
    }

    #[test]
    fn newest_wins_and_tombstones_hide() {
        let newer = VecIter::new(vec![put("a", 10, "a2"), del("b", 11), put("d", 12, "d1")]);
        let older = VecIter::new(vec![put("a", 1, "a1"), put("b", 2, "b1"), put("c", 3, "c1")]);
        let m = MergingIterator::new(vec![Box::new(newer), Box::new(older)]);
        let got = collect(UserIterator::new(m, 100));
        assert_eq!(got, vec![("a".into(), "a2".into()), ("c".into(), "c1".into()), ("d".into(), "d1".into())]);
    }

    #[test]
    fn snapshot_sees_old_state() {
        let newer = VecIter::new(vec![put("a", 10, "a2"), del("b", 11)]);
        let older = VecIter::new(vec![put("a", 1, "a1"), put("b", 2, "b1")]);
        let m = MergingIterator::new(vec![Box::new(newer), Box::new(older)]);
        let got = collect(UserIterator::new(m, 5));
        assert_eq!(got, vec![("a".into(), "a1".into()), ("b".into(), "b1".into())]);
    }
}
