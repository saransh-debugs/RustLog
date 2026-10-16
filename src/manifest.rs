//! MANIFEST: an append-only log of `VersionEdit`s. Replaying it rebuilds
//! the list of live SSTables per level.
//!
//! `CURRENT` names the active MANIFEST. It's replaced atomically (temp file,
//! fsync, rename, fsync dir), so a crash leaves either the old or the new name.
//!
//! An edit is appended and fsynced BEFORE the new version is installed in
//! memory. So a torn last record is an edit nobody ever saw: dropping it on
//! recovery is correct.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;

use crate::coding::{get_slice, get_varint, put_slice, put_varint};
use crate::error::{Error, Result};
use crate::io::{self, FailPoint, FailPoints};
use crate::version::FileMeta;
use crate::wal;

const TAG_LOG_NUMBER: u64 = 1;
const TAG_NEXT_FILE: u64 = 2;
const TAG_LAST_SEQ: u64 = 3;
const TAG_ADD_FILE: u64 = 4;
const TAG_DELETE_FILE: u64 = 5;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionEdit {
    /// WALs with a number below this are fully flushed and can be deleted.
    pub log_number: Option<u64>,
    pub next_file: Option<u64>,
    pub last_seq: Option<u64>,
    pub added: Vec<(usize, FileMeta)>,
    pub deleted: Vec<(usize, u64)>,
}

impl VersionEdit {
    pub fn encode(&self, out: &mut Vec<u8>) {
        if let Some(v) = self.log_number {
            put_varint(out, TAG_LOG_NUMBER);
            put_varint(out, v);
        }
        if let Some(v) = self.next_file {
            put_varint(out, TAG_NEXT_FILE);
            put_varint(out, v);
        }
        if let Some(v) = self.last_seq {
            put_varint(out, TAG_LAST_SEQ);
            put_varint(out, v);
        }
        for (lvl, num) in &self.deleted {
            put_varint(out, TAG_DELETE_FILE);
            put_varint(out, *lvl as u64);
            put_varint(out, *num);
        }
        for (lvl, f) in &self.added {
            put_varint(out, TAG_ADD_FILE);
            put_varint(out, *lvl as u64);
            put_varint(out, f.number);
            put_varint(out, f.size);
            put_slice(out, &f.smallest);
            put_slice(out, &f.largest);
        }
    }

    pub fn decode(mut b: &[u8]) -> Result<VersionEdit> {
        let bad = || Error::corruption("bad MANIFEST edit");
        let mut e = VersionEdit::default();
        let var = |b: &mut &[u8]| -> Result<u64> {
            let (v, n) = get_varint(b).ok_or_else(bad)?;
            *b = &b[n..];
            Ok(v)
        };
        while !b.is_empty() {
            match var(&mut b)? {
                TAG_LOG_NUMBER => e.log_number = Some(var(&mut b)?),
                TAG_NEXT_FILE => e.next_file = Some(var(&mut b)?),
                TAG_LAST_SEQ => e.last_seq = Some(var(&mut b)?),
                TAG_DELETE_FILE => {
                    let lvl = var(&mut b)? as usize;
                    let num = var(&mut b)?;
                    e.deleted.push((lvl, num));
                }
                TAG_ADD_FILE => {
                    let lvl = var(&mut b)? as usize;
                    let number = var(&mut b)?;
                    let size = var(&mut b)?;
                    let (s, n) = get_slice(b).ok_or_else(bad)?;
                    let smallest = Bytes::copy_from_slice(s);
                    b = &b[n..];
                    let (l, n) = get_slice(b).ok_or_else(bad)?;
                    let largest = Bytes::copy_from_slice(l);
                    b = &b[n..];
                    e.added.push((lvl, FileMeta { number, size, smallest, largest }));
                }
                _ => return Err(bad()),
            }
        }
        Ok(e)
    }
}

/// State rebuilt from a MANIFEST.
#[derive(Debug, Default)]
pub struct Recovered {
    pub log_number: u64,
    pub next_file: u64,
    pub last_seq: u64,
    /// level -> file number -> meta
    pub levels: Vec<BTreeMap<u64, FileMeta>>,
}

/// Reads `CURRENT` and replays the MANIFEST it names.
pub fn recover(dir: &Path, num_levels: usize) -> Result<Recovered> {
    let current = std::fs::read_to_string(io::current_path(dir))?;
    let name = current.trim_end_matches('\n');
    let Some(io::FileKind::Manifest(_)) = io::parse_file_name(name) else {
        return Err(Error::corruption(format!("CURRENT names a non-manifest: {name:?}")));
    };
    let mut r = Recovered { levels: vec![BTreeMap::new(); num_levels], ..Default::default() };
    let (_, _, torn) = wal::read_records(&dir.join(name), |payload| {
        let e = VersionEdit::decode(payload)?;
        if let Some(v) = e.log_number {
            r.log_number = v;
        }
        if let Some(v) = e.next_file {
            r.next_file = v;
        }
        if let Some(v) = e.last_seq {
            r.last_seq = v;
        }
        for (lvl, num) in &e.deleted {
            let l = r.levels.get_mut(*lvl).ok_or_else(|| Error::corruption("MANIFEST level out of range"))?;
            l.remove(num);
        }
        for (lvl, f) in e.added {
            let l = r.levels.get_mut(lvl).ok_or_else(|| Error::corruption("MANIFEST level out of range"))?;
            l.insert(f.number, f);
        }
        Ok(())
    })?;
    if torn {
        tracing::warn!("MANIFEST {name} has a torn last record; ignoring it (it was never acknowledged)");
    }
    if r.next_file == 0 {
        return Err(Error::corruption("MANIFEST has no next_file"));
    }
    Ok(r)
}

/// Appends edits to the active MANIFEST.
#[derive(Debug)]
pub struct ManifestWriter {
    file: File,
    fail: Option<Arc<FailPoints>>,
    buf: Vec<u8>,
    scratch: Vec<u8>,
}

impl ManifestWriter {
    /// Creates a new MANIFEST holding `snapshot` and points CURRENT at it.
    pub fn create(dir: &Path, number: u64, snapshot: &VersionEdit, fail: Option<Arc<FailPoints>>) -> Result<Self> {
        let path = io::manifest_path(dir, number);
        let file = File::options().write(true).create_new(true).open(&path)?;
        let mut w = ManifestWriter { file, fail, buf: Vec::new(), scratch: Vec::new() };
        w.append(snapshot)?;
        io::sync_dir(dir)?;
        let name = format!("MANIFEST-{number:06}\n");
        io::write_file_atomic(dir, "CURRENT", name.as_bytes())?;
        Ok(w)
    }

    /// Appends and fsyncs one edit. Only after this returns may the caller
    /// install the new version in memory.
    pub fn append(&mut self, e: &VersionEdit) -> Result<()> {
        io::check(&self.fail, FailPoint::ManifestWrite)?;
        self.scratch.clear();
        e.encode(&mut self.scratch);
        self.buf.clear();
        wal::frame_record(&mut self.buf, &self.scratch);
        self.file.write_all(&self.buf)?;
        self.file.sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edit_roundtrip() {
        let e = VersionEdit {
            log_number: Some(7),
            next_file: Some(42),
            last_seq: Some(1 << 40),
            added: vec![(
                2,
                FileMeta {
                    number: 9,
                    size: 1234,
                    smallest: Bytes::from_static(b"aaaaaaaaaaaa"),
                    largest: Bytes::from_static(b"zzzzzzzzzzzz"),
                },
            )],
            deleted: vec![(1, 3), (1, 4)],
        };
        let mut b = Vec::new();
        e.encode(&mut b);
        assert_eq!(VersionEdit::decode(&b).unwrap(), e);
    }

    #[test]
    fn recover_applies_edits_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let meta = |n: u64| FileMeta {
            number: n,
            size: 10,
            smallest: Bytes::from_static(b"a\0\0\0\0\0\0\0\x01"),
            largest: Bytes::from_static(b"b\0\0\0\0\0\0\0\x01"),
        };
        let snap = VersionEdit { next_file: Some(5), log_number: Some(1), ..Default::default() };
        let mut w = ManifestWriter::create(dir.path(), 2, &snap, None).unwrap();
        w.append(&VersionEdit { added: vec![(0, meta(3)), (0, meta(4))], ..Default::default() }).unwrap();
        w.append(&VersionEdit {
            deleted: vec![(0, 3)],
            added: vec![(1, meta(6))],
            next_file: Some(7),
            ..Default::default()
        })
        .unwrap();
        // A torn trailing write must be ignored.
        let mut f = File::options().append(true).open(io::manifest_path(dir.path(), 2)).unwrap();
        f.write_all(&[1, 2, 3]).unwrap();
        let r = recover(dir.path(), 7).unwrap();
        assert_eq!(r.next_file, 7);
        assert_eq!(r.levels[0].keys().copied().collect::<Vec<_>>(), vec![4]);
        assert_eq!(r.levels[1].keys().copied().collect::<Vec<_>>(), vec![6]);
    }
}
