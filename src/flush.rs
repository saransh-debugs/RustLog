//! The flush thread: writes frozen memtables to L0 SSTables.
//!
//! Readers keep reading the frozen memtable the whole time: it's in the
//! current `SuperVersion` until the very swap that replaces it with the new
//! SSTable. No reader ever waits on a flush.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::engine::{Inner, SuperVersion};
use crate::error::Result;
use crate::iterator::InternalIterator;
use crate::key;
use crate::manifest::VersionEdit;
use crate::memtable::MemTable;
use crate::options::Options;
use crate::sstable::TableBuilder;
use crate::stats;
use crate::version::FileMeta;
use crate::{io, Error};

pub(crate) fn run(inner: Arc<Inner>, rx: crossbeam_channel::Receiver<()>) {
    loop {
        // Timeout so a missed signal can't strand a memtable forever.
        let _ = rx.recv_timeout(Duration::from_millis(200));
        if inner.shutdown.load(std::sync::atomic::Ordering::Acquire) {
            // Frozen memtables are still in their WALs; recovery replays them.
            return;
        }
        while inner.bg_error().is_none() {
            let Some(oldest) = inner.state.load().imms.last().cloned() else { break };
            if let Err(e) = flush_one(&inner, &oldest) {
                inner.set_bg_error(&e);
                break;
            }
        }
    }
}

/// Writes all live entries of `mem` into a new SSTable. Blocking.
///
/// Older versions of a key that a newer version in the same memtable
/// shadows are dropped here. Safe: any reader that could still need them
/// holds a `SuperVersion` that still has this memtable, not the SSTable.
/// Tombstones are kept: they may hide data in older levels.
pub(crate) fn write_memtable(dir: &Path, opts: &Options, number: u64, mem: &Arc<MemTable>) -> Result<FileMeta> {
    let path = io::sst_path(dir, number);
    let res = (|| {
        let mut b = TableBuilder::create(&path, opts.block_size, opts.bloom_bits_per_key, opts.fail_points.clone())?;
        let mut it = mem.iter();
        it.seek_to_first()?;
        let mut last_user: Vec<u8> = Vec::new();
        let mut first = true;
        while it.valid() {
            let uk = key::user_key(it.key());
            if first || uk != last_user.as_slice() {
                b.add(it.key(), it.value())?;
                last_user.clear();
                last_user.extend_from_slice(uk);
                first = false;
            }
            it.next()?;
        }
        let built = b.finish()?;
        Ok(FileMeta { number, size: built.size, smallest: built.smallest, largest: built.largest })
    })();
    if res.is_err() {
        // Don't leave a half-written file behind. Open would delete it as an
        // orphan anyway, but a full disk shouldn't stay full because of us.
        let _ = std::fs::remove_file(&path);
    }
    res
}

fn flush_one(inner: &Arc<Inner>, imm: &Arc<MemTable>) -> Result<()> {
    let number = inner.new_file_number();
    let meta = write_memtable(&inner.dir, &inner.opts, number, imm)?;
    io::sync_dir(&inner.dir)?;
    let table = inner.open_table(meta.clone())?;
    let size = meta.size;

    {
        let mut vs = inner.vset.lock();
        let edit = VersionEdit {
            // Every WAL up to and including this memtable's is now redundant.
            log_number: Some(imm.wal_number + 1),
            next_file: Some(inner.next_file.load(std::sync::atomic::Ordering::SeqCst)),
            last_seq: Some(imm.max_seq()),
            added: vec![(0, meta)],
            deleted: Vec::new(),
        };
        // Durable first, visible second.
        vs.manifest.append(&edit)?;
        let mut install_err: Option<Error> = None;
        inner.update_state(|cur| {
            let version = match cur.version.apply(&[], &[(0, Arc::clone(&table))]) {
                Ok(v) => Arc::new(v),
                Err(e) => {
                    install_err = Some(e);
                    Arc::clone(&cur.version)
                }
            };
            let imms = if install_err.is_none() {
                cur.imms.iter().filter(|m| !Arc::ptr_eq(m, imm)).cloned().collect()
            } else {
                cur.imms.clone()
            };
            SuperVersion { mem: Arc::clone(&cur.mem), imms, version }
        });
        if let Some(e) = install_err {
            return Err(e);
        }
    }

    // The WAL is redundant now. Deleting it is cleanup, not correctness:
    // recovery ignores WALs older than `log_number`.
    if let Err(e) = std::fs::remove_file(io::wal_path(&inner.dir, imm.wal_number)) {
        tracing::warn!("could not delete WAL {}: {e}", imm.wal_number);
    }
    stats::add(&inner.stats.flush_bytes, size);
    stats::add(&inner.stats.flushes, 1);
    tracing::debug!("flushed memtable ({} entries) to {number:06}.sst ({size} bytes)", imm.len());
    inner.wake_all();
    Ok(())
}
