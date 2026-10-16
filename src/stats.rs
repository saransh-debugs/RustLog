//! Counters. All `Relaxed` atomics: they're for reporting, nothing
//! synchronizes on them.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Stats {
    /// Key + value bytes handed to `put`/`delete` by the user.
    pub user_bytes: AtomicU64,
    pub wal_bytes: AtomicU64,
    pub wal_syncs: AtomicU64,
    pub write_groups: AtomicU64,
    pub writes: AtomicU64,
    pub flush_bytes: AtomicU64,
    pub flushes: AtomicU64,
    pub compaction_bytes_read: AtomicU64,
    pub compaction_bytes_written: AtomicU64,
    pub compactions: AtomicU64,
    pub trivial_moves: AtomicU64,
    pub stall_slowdowns: AtomicU64,
    pub stall_stops: AtomicU64,
    pub stall_micros: AtomicU64,
    pub bloom_checks: AtomicU64,
    /// Bloom said "definitely not here": a disk read we skipped.
    pub bloom_negatives: AtomicU64,
    pub disk_block_reads: AtomicU64,
    /// Reads fully served without leaving the async task (memtable or cache).
    pub reads_without_io: AtomicU64,
    pub reads_with_io: AtomicU64,
}

#[inline]
pub(crate) fn add(c: &AtomicU64, v: u64) {
    c.fetch_add(v, Ordering::Relaxed);
}

/// A plain copy of the counters plus the LSM shape, for printing.
#[derive(Debug, Clone, Default)]
pub struct StatsSnapshot {
    pub user_bytes: u64,
    pub wal_bytes: u64,
    pub wal_syncs: u64,
    pub write_groups: u64,
    pub writes: u64,
    pub flush_bytes: u64,
    pub flushes: u64,
    pub compaction_bytes_read: u64,
    pub compaction_bytes_written: u64,
    pub compactions: u64,
    pub trivial_moves: u64,
    pub stall_slowdowns: u64,
    pub stall_stops: u64,
    pub stall_micros: u64,
    pub bloom_checks: u64,
    pub bloom_negatives: u64,
    pub disk_block_reads: u64,
    pub reads_without_io: u64,
    pub reads_with_io: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_used_bytes: u64,
    /// `(file count, bytes)` per level.
    pub levels: Vec<(usize, u64)>,
    pub immutable_memtables: usize,
}

impl StatsSnapshot {
    /// Bytes written to disk (WAL + flush + compaction) per user byte.
    pub fn write_amplification(&self) -> f64 {
        if self.user_bytes == 0 {
            return 0.0;
        }
        (self.wal_bytes + self.flush_bytes + self.compaction_bytes_written) as f64 / self.user_bytes as f64
    }
}

impl Stats {
    pub(crate) fn snapshot(&self) -> StatsSnapshot {
        let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
        StatsSnapshot {
            user_bytes: g(&self.user_bytes),
            wal_bytes: g(&self.wal_bytes),
            wal_syncs: g(&self.wal_syncs),
            write_groups: g(&self.write_groups),
            writes: g(&self.writes),
            flush_bytes: g(&self.flush_bytes),
            flushes: g(&self.flushes),
            compaction_bytes_read: g(&self.compaction_bytes_read),
            compaction_bytes_written: g(&self.compaction_bytes_written),
            compactions: g(&self.compactions),
            trivial_moves: g(&self.trivial_moves),
            stall_slowdowns: g(&self.stall_slowdowns),
            stall_stops: g(&self.stall_stops),
            stall_micros: g(&self.stall_micros),
            bloom_checks: g(&self.bloom_checks),
            bloom_negatives: g(&self.bloom_negatives),
            disk_block_reads: g(&self.disk_block_reads),
            reads_without_io: g(&self.reads_without_io),
            reads_with_io: g(&self.reads_with_io),
            ..Default::default()
        }
    }
}
