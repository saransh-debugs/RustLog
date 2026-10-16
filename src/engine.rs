//! The database: shared state, open/recovery, and the async API.
//!
//! Thread layout:
//! - Tokio tasks (callers): memtable reads, cache-only SST reads, and
//!   sending writes to the writer. They never touch the disk directly.
//! - `spawn_blocking` pool: SST reads that miss the cache, and scans.
//! - `rustlog-writer` thread: WAL appends + fsync (group commit), memtable
//!   inserts, memtable rotation, write stalls.
//! - `rustlog-flush` thread: frozen memtable -> L0 SSTable.
//! - `rustlog-compact` thread: leveled compaction + deleting dead files.
//!
//! Why dedicated threads for flush/compaction instead of `spawn_blocking`?
//! They run for seconds at a time. Long jobs on the blocking pool would sit
//! in the same pool our reads need, and the pool's queue has no priority.

use std::fs::File;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::{Condvar, Mutex};
use tokio::sync::{mpsc, oneshot, watch, Semaphore};

use crate::batch::WriteBatch;
use crate::cache::BlockCache;
use crate::compaction;
use crate::error::{Error, Result};
use crate::flush;
use crate::io::{self, FileKind};
use crate::iterator::{InternalIterator, MergingIterator, UserIterator};
use crate::key::{self, ValueKind, MAX_SEQ};
use crate::manifest::{self, ManifestWriter, VersionEdit};
use crate::memtable::MemTable;
use crate::options::{Options, WriteOptions};
use crate::sstable::{Lookup, ReadMode, Table, TableCtx};
use crate::stats::{self, Stats, StatsSnapshot};
use crate::version::{FileMeta, Version};
use crate::wal::{self, WalWriter};
use crate::writer::{self, WriterMsg};

/// What a reader needs, swapped atomically as one unit.
///
/// Readers do one atomic load (`ArcSwap::load_full`) and then work on a
/// consistent snapshot with no locks. A flush removes a memtable and adds
/// its SSTable in the same swap, so no reader can see the data twice or
/// miss it.
#[derive(Debug)]
pub(crate) struct SuperVersion {
    pub mem: Arc<MemTable>,
    /// Frozen memtables waiting for flush, newest first.
    pub imms: Vec<Arc<MemTable>>,
    pub version: Arc<Version>,
}

/// MANIFEST writer plus compaction bookkeeping. Flush and compaction hold
/// this lock while installing a new version, which serializes all version
/// changes. The writer thread never takes it.
#[derive(Debug)]
pub(crate) struct VersionSet {
    pub manifest: ManifestWriter,
    /// Per level: largest key of the last compaction, so the next one picks
    /// the next file (round-robin over the key space, like LevelDB).
    pub compact_pointer: Vec<Option<Bytes>>,
}

#[derive(Debug)]
pub(crate) struct Inner {
    pub dir: PathBuf,
    pub opts: Options,
    pub state: ArcSwap<SuperVersion>,
    /// Serializes read-modify-write of `state`. Readers never take it.
    pub state_lock: Mutex<()>,
    pub vset: Mutex<VersionSet>,
    pub next_file: AtomicU64,
    /// Highest seq whose write is fully in the memtable. Readers snapshot it.
    pub visible_seq: AtomicU64,
    bg_error: Mutex<Option<Arc<str>>>,
    has_bg_error: AtomicBool,
    /// The writer thread sleeps here during a write stop. Flush and
    /// compaction notify it when they free up room.
    pub stall_mu: Mutex<()>,
    pub stall_cv: Condvar,
    pub flush_tx: crossbeam_channel::Sender<()>,
    pub compact_tx: crossbeam_channel::Sender<()>,
    pub table_ctx: TableCtx,
    /// Bumped after every flush / compaction round, so async callers can
    /// wait for background progress without polling.
    pub bg_progress: watch::Sender<u64>,
    pub cache: Arc<BlockCache>,
    pub stats: Arc<Stats>,
    read_permits: Arc<Semaphore>,
    pub shutdown: AtomicBool,
    _lock_file: File,
}

impl Inner {
    pub fn bg_error(&self) -> Option<Error> {
        if !self.has_bg_error.load(Ordering::Acquire) {
            return None;
        }
        self.bg_error.lock().clone().map(Error::ReadOnly)
    }

    /// Records the first background failure and puts the DB in read-only mode.
    pub fn set_bg_error(&self, e: &Error) {
        {
            let mut g = self.bg_error.lock();
            if g.is_none() {
                tracing::error!("background error, database is now read-only: {e}");
                *g = Some(Arc::from(e.to_string()));
            }
        }
        self.has_bg_error.store(true, Ordering::Release);
        self.wake_all();
    }

    pub fn wake_all(&self) {
        {
            let _g = self.stall_mu.lock();
            self.stall_cv.notify_all();
        }
        let _ = self.flush_tx.try_send(());
        let _ = self.compact_tx.try_send(());
        self.bg_progress.send_modify(|v| *v += 1);
    }

    pub fn new_file_number(&self) -> u64 {
        self.next_file.fetch_add(1, Ordering::SeqCst)
    }

    pub fn open_table(&self, meta: FileMeta) -> Result<Arc<Table>> {
        Ok(Arc::new(Table::open(io::sst_path(&self.dir, meta.number), meta, &self.table_ctx)?))
    }

    /// Replaces `state` with `f(current)`. The only way state changes.
    pub fn update_state(&self, f: impl FnOnce(&SuperVersion) -> SuperVersion) {
        let _g = self.state_lock.lock();
        let cur = self.state.load();
        self.state.store(Arc::new(f(&cur)));
    }

    fn check_writable(&self) -> Result<()> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        self.bg_error().map_or(Ok(()), Err)
    }
}

/// Owns the background threads. Dropping the last `Db` handle drops this,
/// which tells the threads to stop.
#[derive(Debug)]
struct Shared {
    inner: Arc<Inner>,
    tx: mpsc::Sender<WriterMsg>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        // Don't join here: Drop may run on a Tokio worker, and joining
        // threads that are mid-fsync would block it. Threads exit on their
        // own: the writer when its channel closes, the others on the flag.
        // Call `close().await` for a clean, waited-for shutdown.
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.wake_all();
    }
}

/// A handle to an open database. Cheap to clone; all clones share one engine.
#[derive(Debug, Clone)]
pub struct Db {
    shared: Arc<Shared>,
}

impl Db {
    /// Opens (or creates) a database in `path`, recovering from the WAL.
    pub async fn open(path: impl AsRef<Path>, opts: Options) -> Result<Db> {
        opts.validate()?;
        let path = path.as_ref().to_path_buf();
        // Recovery reads and writes files: run it on the blocking pool.
        tokio::task::spawn_blocking(move || open_blocking(path, opts))
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    fn inner(&self) -> &Inner {
        &self.shared.inner
    }

    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let mut b = WriteBatch::new();
        b.put(key, value);
        self.write(b, WriteOptions::default()).await
    }

    pub async fn delete(&self, key: impl Into<Bytes>) -> Result<()> {
        let mut b = WriteBatch::new();
        b.delete(key);
        self.write(b, WriteOptions::default()).await
    }

    /// Applies a batch atomically.
    ///
    /// Backpressure: if the writer's queue is full, this waits (`.await`,
    /// not a blocked thread) until there's room. During a write stall the
    /// queue fills, so callers slow down instead of piling up in RAM.
    ///
    /// If this future is dropped after the batch is queued, the write may
    /// still happen. Same as any database: a dropped request isn't a rollback.
    pub async fn write(&self, batch: WriteBatch, opts: WriteOptions) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let o = &self.inner().opts;
        for (_, k, v) in &batch.ops {
            if k.len() > o.max_key_size {
                return Err(Error::InvalidArgument(format!("key is {} bytes, max is {}", k.len(), o.max_key_size)));
            }
            if v.len() > o.max_value_size {
                return Err(Error::InvalidArgument(format!("value is {} bytes, max is {}", v.len(), o.max_value_size)));
            }
        }
        if batch.size_bytes() > o.memtable_size {
            return Err(Error::InvalidArgument("batch is bigger than a whole memtable".into()));
        }
        self.inner().check_writable()?;
        let (done, rx) = oneshot::channel();
        self.shared.tx.send(WriterMsg::Write { batch, sync: opts.sync, done }).await.map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Reads the newest value of `key`.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let inner = self.inner();
        if inner.shutdown.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        // Load the state BEFORE the sequence number. Every entry in the
        // SSTables and frozen memtables of this state is already visible, so
        // reading `visible_seq` afterwards can't produce a snapshot that is
        // older than data compaction may have already merged away.
        let sv = inner.state.load_full();
        let seq = inner.visible_seq.load(Ordering::Acquire);

        for m in std::iter::once(&sv.mem).chain(sv.imms.iter()) {
            if let Some(r) = m.get(key, seq) {
                stats::add(&inner.stats.reads_without_io, 1);
                return Ok(lookup_to_option(r));
            }
        }

        let sk = key::seek_key(key, seq);
        // Fast path: if every block we need is cached, finish right here.
        // A cache hit costs ~100 ns; a hop to the blocking pool costs
        // several microseconds plus two context switches.
        match sv.version.get(key, &sk, ReadMode::CacheOnly)? {
            Lookup::NeedIo => {}
            r => {
                stats::add(&inner.stats.reads_without_io, 1);
                return Ok(lookup_to_option(r));
            }
        }
        stats::add(&inner.stats.reads_with_io, 1);

        if inner.opts.inline_disk_reads {
            // The anti-pattern, kept only so the benchmark can measure it:
            // a `pread` (or an mmap page fault) right on the async worker.
            // While it waits on the disk, every other task on this worker waits too.
            return Ok(lookup_to_option(sv.version.get(key, &sk, ReadMode::Io { fill_cache: true })?));
        }

        // Cap disk reads in flight. Without this, a burst of 100k gets would
        // queue 100k closures on the blocking pool: unbounded memory, and
        // latency for everyone grows with the queue.
        let permit = Arc::clone(&inner.read_permits).acquire_owned().await.map_err(|_| Error::Closed)?;
        let version = Arc::clone(&sv.version);
        let key = Bytes::copy_from_slice(key);
        let r = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let sk = key::seek_key(&key, seq);
            version.get(&key, &sk, ReadMode::Io { fill_cache: true })
        })
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e)))??;
        Ok(lookup_to_option(r))
    }

    /// Returns up to `limit` live key/value pairs in `range`, in key order.
    ///
    /// `limit` is required: an unbounded scan of a big DB into a `Vec` is an
    /// OOM waiting to happen. Page through with the last key as the next start.
    pub async fn scan<K: AsRef<[u8]>>(&self, range: impl RangeBounds<K>, limit: usize) -> Result<Vec<(Bytes, Bytes)>> {
        let inner = self.inner();
        if inner.shutdown.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let own = |b: Bound<&K>| match b {
            Bound::Included(k) => Bound::Included(Bytes::copy_from_slice(k.as_ref())),
            Bound::Excluded(k) => Bound::Excluded(Bytes::copy_from_slice(k.as_ref())),
            Bound::Unbounded => Bound::Unbounded,
        };
        let (start, end) = (own(range.start_bound()), own(range.end_bound()));
        let sv = inner.state.load_full();
        let seq = inner.visible_seq.load(Ordering::Acquire);
        let permit = Arc::clone(&inner.read_permits).acquire_owned().await.map_err(|_| Error::Closed)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            scan_blocking(&sv, seq, start, end, limit)
        })
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    /// Freezes the current memtable and waits until every frozen memtable
    /// is on disk as an SSTable.
    pub async fn flush(&self) -> Result<()> {
        self.inner().check_writable()?;
        let (done, rx) = oneshot::channel();
        self.shared.tx.send(WriterMsg::Rotate { done }).await.map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)??;
        self.wait_until(|sv| sv.imms.is_empty()).await
    }

    /// Waits until no level needs compaction and nothing is waiting to flush.
    pub async fn wait_for_compaction(&self) -> Result<()> {
        let opts = self.inner().opts.clone();
        self.wait_until(move |sv| sv.imms.is_empty() && compaction::pick(&sv.version, &opts, &[]).is_none()).await
    }

    async fn wait_until(&self, done: impl Fn(&SuperVersion) -> bool) -> Result<()> {
        let inner = self.inner();
        let mut rx = inner.bg_progress.subscribe();
        loop {
            if let Some(e) = inner.bg_error() {
                return Err(e);
            }
            if done(&inner.state.load()) {
                return Ok(());
            }
            if inner.shutdown.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            // Nudge the threads in case the condition depends on them.
            let _ = inner.compact_tx.try_send(());
            let _ = inner.flush_tx.try_send(());
            // Timeout as a safety net against a missed notification.
            let _ = tokio::time::timeout(std::time::Duration::from_millis(50), rx.changed()).await;
        }
    }

    pub fn stats(&self) -> StatsSnapshot {
        let inner = self.inner();
        let mut s = inner.stats.snapshot();
        let sv = inner.state.load();
        s.levels = sv.version.levels.iter().map(|l| (l.len(), l.iter().map(|t| t.meta.size).sum())).collect();
        s.immutable_memtables = sv.imms.len();
        s.cache_hits = inner.cache.hits.load(Ordering::Relaxed);
        s.cache_misses = inner.cache.misses.load(Ordering::Relaxed);
        s.cache_used_bytes = inner.cache.used_bytes() as u64;
        s
    }

    /// The error that put the DB in read-only mode, if any.
    pub fn background_error(&self) -> Option<Error> {
        self.inner().bg_error()
    }

    /// Clean shutdown: every write accepted so far reaches the WAL (and is
    /// fsynced), then all background threads stop and are joined.
    /// Other clones of this handle get `Error::Closed` afterwards.
    pub async fn close(self) -> Result<()> {
        let (done, rx) = oneshot::channel();
        if self.shared.tx.send(WriterMsg::Shutdown { done }).await.is_ok() {
            let _ = rx.await;
        }
        let inner = Arc::clone(&self.shared.inner);
        inner.shutdown.store(true, Ordering::Release);
        inner.wake_all();
        let threads = std::mem::take(&mut *self.shared.threads.lock());
        tokio::task::spawn_blocking(move || {
            for t in threads {
                let _ = t.join();
            }
        })
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e)))?;
        Ok(())
    }

    #[doc(hidden)]
    pub fn check_invariants(&self) -> Result<()> {
        self.inner().state.load().version.check_invariants()
    }
}

fn lookup_to_option(r: Lookup) -> Option<Bytes> {
    match r {
        Lookup::Value(v) => Some(v),
        _ => None,
    }
}

fn scan_blocking(
    sv: &SuperVersion,
    seq: u64,
    start: Bound<Bytes>,
    end: Bound<Bytes>,
    limit: usize,
) -> Result<Vec<(Bytes, Bytes)>> {
    let mut children: Vec<Box<dyn InternalIterator>> = Vec::new();
    children.push(Box::new(sv.mem.iter()));
    for m in &sv.imms {
        children.push(Box::new(m.iter()));
    }
    children.extend(sv.version.iterators(true));
    let mut it = UserIterator::new(MergingIterator::new(children), seq);
    match &start {
        Bound::Included(k) | Bound::Excluded(k) => it.seek(k)?,
        Bound::Unbounded => it.seek_to_first()?,
    }
    if let Bound::Excluded(k) = &start {
        if it.valid() && it.key() == k.as_ref() {
            it.next()?;
        }
    }
    let mut out = Vec::new();
    while it.valid() && out.len() < limit {
        let past_end = match &end {
            Bound::Included(e) => it.key() > e.as_ref(),
            Bound::Excluded(e) => it.key() >= e.as_ref(),
            Bound::Unbounded => false,
        };
        if past_end {
            break;
        }
        out.push((Bytes::copy_from_slice(it.key()), it.value().clone()));
        it.next()?;
    }
    Ok(out)
}

/// Open + recovery. Blocking; runs on the blocking pool.
///
/// 1. Take the LOCK file so two processes can't open the same DB.
/// 2. Replay the MANIFEST to learn which SSTables exist.
/// 3. Replay WALs newer than the MANIFEST's log number into a memtable.
/// 4. Write that memtable out as an L0 SSTable.
/// 5. Write a fresh MANIFEST (snapshot of everything), switch CURRENT to it.
/// 6. Delete files nothing references: old WALs, old MANIFEST, orphan SSTs
///    left by a compaction that crashed before it was committed.
fn open_blocking(dir: PathBuf, opts: Options) -> Result<Db> {
    std::fs::create_dir_all(&dir)?;
    let lock_file = File::options().create(true).truncate(false).write(true).open(io::lock_path(&dir))?;
    match lock_file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Err(Error::Locked),
        Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
    }

    let mut existing: Vec<(FileKind, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if let Some(kind) = name.to_str().and_then(io::parse_file_name) {
            existing.push((kind, entry.path()));
        }
    }

    let recovered = if io::current_path(&dir).exists() {
        manifest::recover(&dir, opts.num_levels)?
    } else {
        if existing.iter().any(|(k, _)| matches!(k, FileKind::Sst(_) | FileKind::Manifest(_))) {
            return Err(Error::corruption("SSTables or MANIFEST present but CURRENT is missing"));
        }
        manifest::Recovered { next_file: 1, levels: vec![Default::default(); opts.num_levels], ..Default::default() }
    };

    let cache = Arc::new(BlockCache::new(opts.block_cache_bytes));
    let stats = Arc::new(Stats::default());
    let (deleter_tx, deleter_rx) = crossbeam_channel::unbounded::<PathBuf>();
    let (flush_tx, flush_rx) = crossbeam_channel::bounded::<()>(1);
    let (compact_tx, compact_rx) = crossbeam_channel::bounded::<()>(1);
    let table_ctx = TableCtx {
        cache: Arc::clone(&cache),
        stats: Arc::clone(&stats),
        deleter: Some(deleter_tx.clone()),
        bypass_os_cache: opts.bypass_os_cache,
        simulated_read_latency: opts.simulated_read_latency,
    };

    // Highest file number in use, so new files never collide with orphans.
    let max_existing = existing
        .iter()
        .filter_map(|(k, _)| match k {
            FileKind::Wal(n) | FileKind::Sst(n) | FileKind::Manifest(n) => Some(*n),
            FileKind::Temp => None,
        })
        .max()
        .unwrap_or(0);
    let next_file = AtomicU64::new(recovered.next_file.max(max_existing + 1));

    // Open every live table.
    let mut version = Version::new(opts.num_levels);
    for (lvl, files) in recovered.levels.iter().enumerate() {
        for meta in files.values() {
            let t = Table::open(io::sst_path(&dir, meta.number), meta.clone(), &table_ctx)?;
            version.levels[lvl].push(Arc::new(t));
        }
    }
    let mut version = version.sorted();
    version.check_invariants()?;

    // Replay WALs, oldest first. Only the newest may end in a torn record.
    let mut wals: Vec<u64> = existing
        .iter()
        .filter_map(|(k, _)| match k {
            FileKind::Wal(n) if *n >= recovered.log_number => Some(*n),
            _ => None,
        })
        .collect();
    wals.sort_unstable();
    let replay_mem = Arc::new(MemTable::new(0));
    let mut last_seq = recovered.last_seq;
    for (i, &num) in wals.iter().enumerate() {
        let path = io::wal_path(&dir, num);
        let out = wal::replay(&path, |seq, kind, k, v| {
            replay_mem.insert(seq, kind, k, Bytes::copy_from_slice(v));
        })?;
        if out.stopped_early {
            if i + 1 != wals.len() {
                return Err(Error::corruption(format!(
                    "WAL {num} is damaged at byte {} but is not the newest log",
                    out.valid_len
                )));
            }
            tracing::warn!("WAL {num}: dropped a torn tail after byte {} (unacknowledged write)", out.valid_len);
        }
        last_seq = last_seq.max(out.max_seq);
    }
    if last_seq > MAX_SEQ {
        return Err(Error::corruption("sequence number overflow"));
    }

    // Recovered writes -> one L0 table, so the old WALs can go.
    if !replay_mem.is_empty() {
        let number = next_file.fetch_add(1, Ordering::SeqCst);
        let meta = flush::write_memtable(&dir, &opts, number, &replay_mem)?;
        let t = Table::open(io::sst_path(&dir, number), meta, &table_ctx)?;
        version.levels[0].push(Arc::new(t));
        version = version.sorted();
        io::sync_dir(&dir)?;
    }

    // New WAL first, then the MANIFEST that says "replay from this WAL on".
    let wal_number = next_file.fetch_add(1, Ordering::SeqCst);
    let wal = WalWriter::create(&dir, wal_number, opts.fail_points.clone())?;
    let manifest_number = next_file.fetch_add(1, Ordering::SeqCst);
    let snapshot = VersionEdit {
        log_number: Some(wal_number),
        next_file: Some(next_file.load(Ordering::SeqCst)),
        last_seq: Some(last_seq),
        added: version
            .levels
            .iter()
            .enumerate()
            .flat_map(|(lvl, files)| files.iter().map(move |t| (lvl, t.meta.clone())))
            .collect(),
        deleted: Vec::new(),
    };
    let manifest = ManifestWriter::create(&dir, manifest_number, &snapshot, opts.fail_points.clone())?;

    // Garbage: anything not referenced by the new MANIFEST.
    let live_ssts: std::collections::HashSet<u64> = version.levels.iter().flatten().map(|t| t.number()).collect();
    for (kind, path) in &existing {
        let dead = match kind {
            FileKind::Wal(n) => *n != wal_number,
            FileKind::Sst(n) => !live_ssts.contains(n),
            FileKind::Manifest(n) => *n != manifest_number,
            FileKind::Temp => true,
        };
        if dead {
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!("could not delete {}: {e}", path.display());
            }
        }
    }
    io::sync_dir(&dir)?;

    let mem = Arc::new(MemTable::new(wal_number));
    let state = SuperVersion { mem: Arc::clone(&mem), imms: Vec::new(), version: Arc::new(version) };
    let num_levels = opts.num_levels;
    let read_permits = Arc::new(Semaphore::new(opts.max_concurrent_disk_reads));
    let (bg_progress, _) = watch::channel(0u64);
    let inner = Arc::new(Inner {
        dir,
        state: ArcSwap::from_pointee(state),
        state_lock: Mutex::new(()),
        vset: Mutex::new(VersionSet { manifest, compact_pointer: vec![None; num_levels] }),
        next_file,
        visible_seq: AtomicU64::new(last_seq),
        bg_error: Mutex::new(None),
        has_bg_error: AtomicBool::new(false),
        stall_mu: Mutex::new(()),
        stall_cv: Condvar::new(),
        flush_tx,
        compact_tx,
        table_ctx,
        bg_progress,
        cache,
        stats,
        read_permits,
        shutdown: AtomicBool::new(false),
        _lock_file: lock_file,
        opts,
    });

    let (tx, rx) = mpsc::channel(inner.opts.write_queue_depth);
    let mut threads = Vec::new();
    let spawn = |name: &str, f: Box<dyn FnOnce() + Send>| std::thread::Builder::new().name(name.to_string()).spawn(f);
    {
        let i = Arc::clone(&inner);
        threads.push(spawn("rustlog-writer", Box::new(move || writer::run(i, rx, wal, mem, last_seq)))?);
    }
    {
        let i = Arc::clone(&inner);
        threads.push(spawn("rustlog-flush", Box::new(move || flush::run(i, flush_rx)))?);
    }
    {
        let i = Arc::clone(&inner);
        threads.push(spawn("rustlog-compact", Box::new(move || compaction::run(i, compact_rx, deleter_rx)))?);
    }
    // Recovery may have left L0 over the trigger.
    let _ = inner.compact_tx.try_send(());

    Ok(Db { shared: Arc::new(Shared { inner, tx, threads: Mutex::new(threads) }) })
}

/// Shared by recovery and the writer: insert a batch with consecutive seqs.
pub(crate) fn insert_batch(mem: &MemTable, first_seq: u64, batch: &WriteBatch) {
    for (i, (kind, k, v)) in batch.ops.iter().enumerate() {
        let v = if *kind == ValueKind::Delete { Bytes::new() } else { v.clone() };
        mem.insert(first_seq + i as u64, *kind, k, v);
    }
}

#[allow(dead_code)]
fn _assert_send_sync() {
    fn f<T: Send + Sync>() {}
    f::<Db>();
}
