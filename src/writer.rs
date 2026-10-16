//! The writer thread: group commit, memtable rotation, write stalls.
//!
//! One thread owns the WAL and the active memtable. Callers send batches
//! through a bounded channel and await a oneshot reply.
//!
//! Why not let each caller write the WAL under a `Mutex`? Then N concurrent
//! `sync` writes cost N fsyncs, back to back, each ~0.1-10 ms. Here the
//! writer takes everything queued, writes it with ONE `write` and ONE fsync,
//! then answers all of them. Fsync cost is shared by the whole group.
//!
//! This thread blocks (on fsync, on stalls). That's the point of giving it
//! its own OS thread: a blocked Tokio worker would freeze every task on it.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use crate::batch::WriteBatch;
use crate::engine::{insert_batch, Inner, SuperVersion};
use crate::error::{Error, Result};
use crate::memtable::MemTable;
use crate::stats;
use crate::wal::{self, WalWriter};

#[derive(Debug)]
pub(crate) enum WriterMsg {
    Write {
        batch: WriteBatch,
        sync: bool,
        done: oneshot::Sender<Result<()>>,
    },
    /// Freeze the memtable now (for `Db::flush`).
    Rotate {
        done: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        done: oneshot::Sender<()>,
    },
}

struct Writer {
    inner: Arc<Inner>,
    wal: WalWriter,
    mem: Arc<MemTable>,
    last_seq: u64,
    /// Reused across groups: steady state does no allocation for framing.
    buf: Vec<u8>,
    scratch: Vec<u8>,
}

pub(crate) fn run(
    inner: Arc<Inner>,
    mut rx: mpsc::Receiver<WriterMsg>,
    wal: WalWriter,
    mem: Arc<MemTable>,
    last_seq: u64,
) {
    let mut w = Writer { inner, wal, mem, last_seq, buf: Vec::with_capacity(1 << 20), scratch: Vec::new() };
    let mut group: Vec<(WriteBatch, bool, oneshot::Sender<Result<()>>)> = Vec::new();
    let max_group = w.inner.opts.max_write_group_bytes;

    'outer: while let Some(first) = rx.blocking_recv() {
        let mut pending = Some(first);
        let mut bytes = 0usize;
        // Build a group: the first message plus whatever else is already
        // queued, up to `max_group` bytes. We never wait for more writes to
        // arrive; a lone write goes out alone, so latency stays low at low load.
        while let Some(msg) = pending.take() {
            match msg {
                WriterMsg::Write { batch, sync, done } => {
                    bytes += batch.size_bytes();
                    group.push((batch, sync, done));
                }
                WriterMsg::Rotate { done } => {
                    // Commit what we have first, so the flush includes it.
                    w.commit(&mut group);
                    let _ = done.send(w.make_room(true));
                }
                WriterMsg::Shutdown { done } => {
                    w.commit(&mut group);
                    let _ = done.send(());
                    break 'outer;
                }
            }
            if bytes < max_group {
                pending = rx.try_recv().ok();
            }
        }
        w.commit(&mut group);
    }

    // Leaving: make what's in the WAL durable. Data in the OS page cache
    // survives a process crash, but not a power cut.
    if w.inner.bg_error().is_none() {
        if let Err(e) = w.wal.sync() {
            w.inner.set_bg_error(&e);
        }
    }
}

impl Writer {
    fn commit(&mut self, group: &mut Vec<(WriteBatch, bool, oneshot::Sender<Result<()>>)>) {
        if group.is_empty() {
            return;
        }
        let res = self.make_room(false).and_then(|()| self.write_group(group));
        for (_, _, done) in group.drain(..) {
            // A closed receiver means the caller gave up waiting. Nothing to do.
            let _ = done.send(match &res {
                Ok(()) => Ok(()),
                Err(e) => Err(clone_err(e)),
            });
        }
    }

    fn write_group(&mut self, group: &[(WriteBatch, bool, oneshot::Sender<Result<()>>)]) -> Result<()> {
        if let Some(e) = self.inner.bg_error() {
            return Err(e);
        }
        self.buf.clear();
        let mut seq = self.last_seq;
        let mut need_sync = false;
        let mut user_bytes = 0;
        let mut writes = 0;
        for (batch, sync, _) in group {
            wal::append_batch_record(&mut self.buf, &mut self.scratch, seq + 1, batch);
            seq += batch.len() as u64;
            need_sync |= *sync;
            user_bytes += batch.size_bytes() as u64;
            writes += batch.len() as u64;
        }
        if seq > crate::key::MAX_SEQ {
            return Err(Error::InvalidArgument("sequence numbers exhausted".into()));
        }
        // WAL first. If this fails, nothing reached the memtable, so readers
        // never see a write that isn't in the log.
        let io = self.wal.write(&self.buf).and_then(|()| if need_sync { self.wal.sync() } else { Ok(()) });
        if let Err(e) = io {
            self.inner.set_bg_error(&e);
            return Err(e);
        }
        let s = &self.inner.stats;
        stats::add(&s.wal_bytes, self.buf.len() as u64);
        stats::add(&s.user_bytes, user_bytes);
        stats::add(&s.writes, writes);
        stats::add(&s.write_groups, 1);
        if need_sync {
            stats::add(&s.wal_syncs, 1);
        }

        let mut next = self.last_seq + 1;
        for (batch, _, _) in group {
            insert_batch(&self.mem, next, batch);
            next += batch.len() as u64;
        }
        self.last_seq = seq;
        // Publish: readers that load `visible_seq` after this see the group.
        // Release pairs with the readers' Acquire load.
        self.inner.visible_seq.store(seq, Ordering::Release);
        Ok(())
    }

    /// Makes sure the memtable has room, rotating and stalling as needed.
    /// Same logic as LevelDB's `MakeRoomForWrite`.
    fn make_room(&mut self, force: bool) -> Result<()> {
        let opts = &self.inner.opts;
        let mut allow_delay = !force;
        loop {
            if let Some(e) = self.inner.bg_error() {
                return Err(e);
            }
            if self.inner.shutdown.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            let sv = self.inner.state.load_full();
            let l0 = sv.version.levels.first().map_or(0, Vec::len);

            if allow_delay && l0 >= opts.l0_slowdown_trigger {
                // Soft limit: delay this group by 1 ms. Spreads the pain over
                // many writes instead of one multi-second stop later.
                let t = Instant::now();
                std::thread::sleep(Duration::from_millis(1));
                stats::add(&self.inner.stats.stall_slowdowns, 1);
                stats::add(&self.inner.stats.stall_micros, t.elapsed().as_micros() as u64);
                allow_delay = false;
                continue;
            }
            if !force && self.mem.approximate_size() < opts.memtable_size {
                return Ok(());
            }
            if force && self.mem.is_empty() {
                return Ok(());
            }
            if sv.imms.len() >= opts.max_immutable_memtables {
                tracing::debug!("write stop: {} memtables waiting for flush", sv.imms.len());
                self.wait_for_background(|sv| sv.imms.len() >= opts.max_immutable_memtables);
                continue;
            }
            if l0 >= opts.l0_stop_trigger {
                tracing::debug!("write stop: {l0} L0 files");
                self.wait_for_background(|sv| sv.version.levels.first().map_or(0, Vec::len) >= opts.l0_stop_trigger);
                continue;
            }
            return self.rotate();
        }
    }

    /// Hard stop: block this thread until flush/compaction makes room.
    /// Callers aren't blocked; they're awaiting. The bounded queue fills
    /// up and new writers wait in `send().await`. That's the backpressure.
    fn wait_for_background(&self, still_stalled: impl Fn(&SuperVersion) -> bool) {
        let t = Instant::now();
        stats::add(&self.inner.stats.stall_stops, 1);
        let _ = self.inner.flush_tx.try_send(());
        let _ = self.inner.compact_tx.try_send(());
        let mut g = self.inner.stall_mu.lock();
        // Re-check under the lock: a flush that finished between our check
        // and here has already notified, and we'd sleep through it.
        if still_stalled(&self.inner.state.load())
            && self.inner.bg_error().is_none()
            && !self.inner.shutdown.load(Ordering::Acquire)
        {
            // Timeout = belt and braces against a missed wakeup.
            self.inner.stall_cv.wait_for(&mut g, Duration::from_millis(100));
        }
        stats::add(&self.inner.stats.stall_micros, t.elapsed().as_micros() as u64);
    }

    /// Freezes the memtable and starts a new WAL.
    fn rotate(&mut self) -> Result<()> {
        // Sync the old WAL before leaving it. Recovery relies on this: only
        // the NEWEST log may have a torn tail. Any damage in an older log is
        // then real corruption, not a crash artifact.
        let res = self.wal.sync().and_then(|()| {
            let number = self.inner.new_file_number();
            WalWriter::create(&self.inner.dir, number, self.inner.opts.fail_points.clone())
        });
        let new_wal = match res {
            Ok(w) => w,
            Err(e) => {
                self.inner.set_bg_error(&e);
                return Err(e);
            }
        };
        self.wal = new_wal;
        let new_mem = Arc::new(MemTable::new(self.wal.number));
        let old = std::mem::replace(&mut self.mem, Arc::clone(&new_mem));
        self.inner.update_state(|cur| {
            let mut imms = Vec::with_capacity(cur.imms.len() + 1);
            imms.push(old);
            imms.extend(cur.imms.iter().cloned());
            SuperVersion { mem: new_mem, imms, version: Arc::clone(&cur.version) }
        });
        let _ = self.inner.flush_tx.try_send(());
        Ok(())
    }
}

/// `Error` holds an `io::Error`, which isn't `Clone`. Every member of a
/// failed group gets the same message.
fn clone_err(e: &Error) -> Error {
    match e {
        Error::Io(io) => Error::Io(std::io::Error::new(io.kind(), io.to_string())),
        Error::Corruption(s) => Error::Corruption(s.clone()),
        Error::ReadOnly(s) => Error::ReadOnly(s.clone()),
        Error::InvalidArgument(s) => Error::InvalidArgument(s.clone()),
        Error::Locked => Error::Locked,
        Error::Closed => Error::Closed,
    }
}
