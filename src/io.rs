//! File helpers: fsync, directory fsync, file naming, and test fail points.
//!
//! Everything in here BLOCKS. It's only ever called from our own threads
//! (writer / flush / compaction) or from inside `spawn_blocking`, never from
//! an async task directly.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Places where a test can make I/O fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailPoint {
    WalWrite,
    WalSync,
    SstWrite,
    ManifestWrite,
}

/// Test-only fault injection. Simulates a full disk or a dying device.
#[derive(Debug, Default)]
pub struct FailPoints {
    wal_write: AtomicBool,
    wal_sync: AtomicBool,
    sst_write: AtomicBool,
    manifest_write: AtomicBool,
    /// Counts injected failures, so tests can check the point was hit.
    pub hits: AtomicU64,
}

impl FailPoints {
    fn flag(&self, p: FailPoint) -> &AtomicBool {
        match p {
            FailPoint::WalWrite => &self.wal_write,
            FailPoint::WalSync => &self.wal_sync,
            FailPoint::SstWrite => &self.sst_write,
            FailPoint::ManifestWrite => &self.manifest_write,
        }
    }

    pub fn set(&self, p: FailPoint, on: bool) {
        self.flag(p).store(on, Ordering::SeqCst);
    }
}

#[inline]
pub(crate) fn check(fp: &Option<std::sync::Arc<FailPoints>>, p: FailPoint) -> io::Result<()> {
    if let Some(fp) = fp {
        if fp.flag(p).load(Ordering::SeqCst) {
            fp.hits.fetch_add(1, Ordering::SeqCst);
            // ENOSPC is the realistic failure: the disk filled up.
            return Err(io::Error::new(io::ErrorKind::StorageFull, "injected failure"));
        }
    }
    Ok(())
}

/// fsync a directory so that a file we just created/renamed/deleted inside
/// it survives a crash. Without this, the file's data can be durable while
/// its directory entry is not, and after reboot the file is just gone.
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

pub fn wal_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{number:06}.log"))
}

pub fn sst_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{number:06}.sst"))
}

pub fn manifest_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("MANIFEST-{number:06}"))
}

pub fn current_path(dir: &Path) -> PathBuf {
    dir.join("CURRENT")
}

pub fn lock_path(dir: &Path) -> PathBuf {
    dir.join("LOCK")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Wal(u64),
    Sst(u64),
    Manifest(u64),
    Temp,
}

/// Parses a file name in the DB directory. Unknown names return `None`
/// and are left alone.
pub fn parse_file_name(name: &str) -> Option<FileKind> {
    if let Some(n) = name.strip_suffix(".log") {
        return n.parse().ok().map(FileKind::Wal);
    }
    if let Some(n) = name.strip_suffix(".sst") {
        return n.parse().ok().map(FileKind::Sst);
    }
    if let Some(n) = name.strip_prefix("MANIFEST-") {
        return n.parse().ok().map(FileKind::Manifest);
    }
    if name.ends_with(".tmp") {
        return Some(FileKind::Temp);
    }
    None
}

/// Writes `data` to `path` atomically: write a temp file, fsync it, rename
/// over the target, fsync the directory. A crash leaves either the old file
/// or the new one, never a half-written one.
pub fn write_file_atomic(dir: &Path, name: &str, data: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(name))?;
    sync_dir(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_names() {
        assert_eq!(parse_file_name("000012.log"), Some(FileKind::Wal(12)));
        assert_eq!(parse_file_name("000007.sst"), Some(FileKind::Sst(7)));
        assert_eq!(parse_file_name("MANIFEST-000003"), Some(FileKind::Manifest(3)));
        assert_eq!(parse_file_name("CURRENT.tmp"), Some(FileKind::Temp));
        assert_eq!(parse_file_name("notes.txt"), None);
        assert_eq!(parse_file_name("abc.sst"), None);
    }
}
