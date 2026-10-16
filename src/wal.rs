//! Write-ahead log.
//!
//! Record framing (also used by the MANIFEST):
//! ```text
//! crc: u32 (masked crc32c of len + payload) | len: u32 | payload
//! ```
//! WAL payload = one write batch:
//! ```text
//! first_seq: u64 | count: u32 | (kind: u8 | key: varint-len bytes | value: varint-len bytes)*
//! ```
//!
//! On recovery we read records until the first one that is short or fails
//! its checksum. In the newest log that's a torn write from the crash: the
//! write was never acknowledged, so dropping it is correct. Anywhere else it's
//! real corruption, because older logs are fsynced before we switch away.

use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use crate::batch::WriteBatch;
use crate::coding::{get_slice, get_u32, get_u64, mask_crc, masked_crc, put_slice, put_u32, put_u64};
use crate::error::{Error, Result};
use crate::io::{self, FailPoint, FailPoints};
use crate::key::ValueKind;

pub const HEADER_LEN: usize = 8;
/// Refuse to even try to allocate a record bigger than this on recovery.
/// A corrupt length field must not turn into a 4 GiB allocation.
pub const MAX_RECORD: usize = 256 << 20;

/// Appends one framed record to `buf`.
pub fn frame_record(buf: &mut Vec<u8>, payload: &[u8]) {
    let len = (payload.len() as u32).to_le_bytes();
    // CRC covers the length too, so a flipped length bit is caught.
    let crc = mask_crc(crc32c::crc32c_append(crc32c::crc32c(&len), payload));
    put_u32(buf, crc);
    buf.extend_from_slice(&len);
    buf.extend_from_slice(payload);
}

/// Encodes a batch as a WAL payload directly into the frame buffer, so
/// a group commit is one contiguous `write`.
pub fn append_batch_record(buf: &mut Vec<u8>, scratch: &mut Vec<u8>, first_seq: u64, batch: &WriteBatch) {
    scratch.clear();
    put_u64(scratch, first_seq);
    put_u32(scratch, batch.ops.len() as u32);
    for (kind, k, v) in &batch.ops {
        scratch.push(*kind as u8);
        put_slice(scratch, k);
        put_slice(scratch, v);
    }
    frame_record(buf, scratch);
}

/// Result of reading one log.
#[derive(Debug)]
pub struct ReadOutcome {
    #[cfg_attr(not(test), allow(dead_code))]
    pub records: usize,
    pub max_seq: u64,
    /// Bytes of intact records. Anything after this is torn or corrupt.
    pub valid_len: u64,
    /// True if the file had bytes after the last intact record.
    pub stopped_early: bool,
}

/// Reads framed records from a file. Calls `f` with each intact payload.
pub fn read_records(path: &Path, mut f: impl FnMut(&[u8]) -> Result<()>) -> Result<(usize, u64, bool)> {
    let data = std::fs::read(path)?;
    let mut off = 0usize;
    let mut n = 0usize;
    loop {
        let rest = &data[off..];
        if rest.is_empty() {
            return Ok((n, off as u64, false));
        }
        let (Some(crc), Some(len)) = (get_u32(rest, 0), get_u32(rest, 4)) else {
            return Ok((n, off as u64, true));
        };
        let len = len as usize;
        if len > MAX_RECORD || rest.len() < HEADER_LEN + len {
            return Ok((n, off as u64, true));
        }
        if masked_crc(&rest[4..HEADER_LEN + len]) != crc {
            return Ok((n, off as u64, true));
        }
        f(&rest[HEADER_LEN..HEADER_LEN + len])?;
        off += HEADER_LEN + len;
        n += 1;
    }
}

/// Replays one WAL file. `apply(seq, kind, key, value)` is called per op.
pub fn replay(path: &Path, mut apply: impl FnMut(u64, ValueKind, &[u8], &[u8])) -> Result<ReadOutcome> {
    let mut max_seq = 0;
    let (records, valid_len, stopped_early) = read_records(path, |p| {
        let bad = || Error::corruption("bad WAL batch payload");
        let first = get_u64(p, 0).ok_or_else(bad)?;
        let count = get_u32(p, 8).ok_or_else(bad)?;
        let mut rest = &p[12..];
        for i in 0..u64::from(count) {
            let (&kind, r) = rest.split_first().ok_or_else(bad)?;
            let kind = ValueKind::from_u8(kind).ok_or_else(bad)?;
            let (k, a) = get_slice(r).ok_or_else(bad)?;
            let (v, b) = get_slice(&r[a..]).ok_or_else(bad)?;
            let seq = first + i;
            apply(seq, kind, k, v);
            max_seq = max_seq.max(seq);
            rest = &r[a + b..];
        }
        Ok(())
    })?;
    Ok(ReadOutcome { records, max_seq, valid_len, stopped_early })
}

/// The open log. Owned by the writer thread only, so no locking.
#[derive(Debug)]
pub struct WalWriter {
    file: File,
    pub number: u64,
    fail: Option<Arc<FailPoints>>,
}

impl WalWriter {
    pub fn create(dir: &Path, number: u64, fail: Option<Arc<FailPoints>>) -> Result<Self> {
        let path = io::wal_path(dir, number);
        let file = File::options().write(true).create_new(true).open(&path)?;
        // The new file's directory entry must be durable too, or a crash can
        // lose the whole log even though its contents were fsynced.
        io::sync_dir(dir)?;
        Ok(WalWriter { file, number, fail })
    }

    /// One `write` for a whole group of records.
    pub fn write(&mut self, buf: &[u8]) -> Result<()> {
        io::check(&self.fail, FailPoint::WalWrite)?;
        self.file.write_all(buf)?;
        Ok(())
    }

    /// `fdatasync`: data only, skip the inode timestamp update. On macOS,
    /// Rust's `sync_data` uses `F_FULLFSYNC`, the only call there that
    /// actually flushes the drive's cache.
    pub fn sync(&mut self) -> Result<()> {
        io::check(&self.fail, FailPoint::WalSync)?;
        self.file.sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_batches(dir: &Path, n: usize) -> (std::path::PathBuf, Vec<u64>) {
        let mut w = WalWriter::create(dir, 1, None).unwrap();
        let mut buf = Vec::new();
        let mut scratch = Vec::new();
        let mut ends = Vec::new();
        let mut total = 0u64;
        for i in 0..n {
            let mut b = WriteBatch::new();
            b.put(format!("k{i}"), format!("v{i}"));
            if i % 3 == 0 {
                b.delete(format!("d{i}"));
            }
            buf.clear();
            append_batch_record(&mut buf, &mut scratch, i as u64 * 10 + 1, &b);
            w.write(&buf).unwrap();
            total += buf.len() as u64;
            ends.push(total);
        }
        w.sync().unwrap();
        (io::wal_path(dir, 1), ends)
    }

    #[test]
    fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let (p, _) = write_batches(dir.path(), 100);
        let mut ops = 0;
        let out = replay(&p, |_, _, _, _| ops += 1).unwrap();
        assert_eq!(out.records, 100);
        assert_eq!(ops, 100 + 34);
        assert!(!out.stopped_early);
    }

    #[test]
    fn bit_flip_stops_replay_at_that_record() {
        let dir = tempfile::tempdir().unwrap();
        let (p, ends) = write_batches(dir.path(), 10);
        let mut data = std::fs::read(&p).unwrap();
        let pos = ends[4] as usize + 10; // inside record 5
        data[pos] ^= 1;
        std::fs::write(&p, &data).unwrap();
        let out = replay(&p, |_, _, _, _| {}).unwrap();
        assert_eq!(out.records, 5);
        assert!(out.stopped_early);
    }
}
