use std::sync::Arc;

use crate::io::FailPoints;

/// Engine settings. Defaults are sized for a laptop, not a server.
#[derive(Debug, Clone)]
pub struct Options {
    /// Active MemTable size before it's frozen and flushed.
    pub memtable_size: usize,
    /// Frozen MemTables waiting for flush. Hitting this stops writes.
    /// Each one is RAM we can't free until it's on disk, so this caps memory.
    pub max_immutable_memtables: usize,
    /// Target size of one SSTable data block (before the checksum).
    pub block_size: usize,
    /// Bloom filter bits per key. 10 gives ~1% false positives. 0 disables.
    pub bloom_bits_per_key: usize,
    /// Block cache size in bytes. This is a hard cap, unlike mmap.
    pub block_cache_bytes: usize,
    /// Compaction output files are cut at about this size.
    pub target_file_size: u64,
    /// L0 file count that triggers an L0 -> L1 compaction.
    pub l0_compaction_trigger: usize,
    /// L0 file count where writes get slowed down (1 ms per write group).
    pub l0_slowdown_trigger: usize,
    /// L0 file count where writes stop until compaction catches up.
    pub l0_stop_trigger: usize,
    /// Max bytes in L1. Each next level is `level_multiplier` times bigger.
    pub level_base_bytes: u64,
    pub level_multiplier: u64,
    /// Number of levels including L0.
    pub num_levels: usize,
    /// Cap on concurrent disk reads in flight on the blocking pool.
    pub max_concurrent_disk_reads: usize,
    /// Pending writes the writer thread will queue before callers wait.
    pub write_queue_depth: usize,
    /// One group commit takes at most this many bytes of writes.
    pub max_write_group_bytes: usize,
    /// Largest key accepted. Keys live in index blocks and in RAM, so huge
    /// keys are an OOM risk.
    pub max_key_size: usize,
    /// Largest value accepted.
    pub max_value_size: usize,

    /// DEMO ONLY: read SSTables directly on the async worker thread instead
    /// of `spawn_blocking`. Exists so the benchmark can show the damage.
    #[doc(hidden)]
    pub inline_disk_reads: bool,
    /// TEST ONLY: make chosen I/O calls fail.
    #[doc(hidden)]
    pub fail_points: Option<Arc<FailPoints>>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            memtable_size: 4 << 20,
            max_immutable_memtables: 2,
            block_size: 4096,
            bloom_bits_per_key: 10,
            block_cache_bytes: 64 << 20,
            target_file_size: 2 << 20,
            l0_compaction_trigger: 4,
            l0_slowdown_trigger: 8,
            l0_stop_trigger: 12,
            level_base_bytes: 10 << 20,
            level_multiplier: 10,
            num_levels: 7,
            max_concurrent_disk_reads: 64,
            write_queue_depth: 1024,
            max_write_group_bytes: 1 << 20,
            max_key_size: 64 << 10,
            max_value_size: 16 << 20,
            inline_disk_reads: false,
            fail_points: None,
        }
    }
}

impl Options {
    pub(crate) fn validate(&self) -> crate::Result<()> {
        let bad = |m: &str| Err(crate::Error::InvalidArgument(m.to_string()));
        if self.memtable_size < 4096 {
            return bad("memtable_size must be at least 4 KiB");
        }
        if self.max_immutable_memtables == 0 {
            return bad("max_immutable_memtables must be >= 1");
        }
        if self.block_size < 256 {
            return bad("block_size must be at least 256");
        }
        if !(self.l0_compaction_trigger <= self.l0_slowdown_trigger && self.l0_slowdown_trigger <= self.l0_stop_trigger)
        {
            return bad("need l0_compaction_trigger <= l0_slowdown_trigger <= l0_stop_trigger");
        }
        if self.l0_compaction_trigger == 0 {
            return bad("l0_compaction_trigger must be >= 1");
        }
        if !(2..=16).contains(&self.num_levels) {
            return bad("num_levels must be in 2..=16");
        }
        if self.level_multiplier < 2 {
            return bad("level_multiplier must be >= 2");
        }
        if self.max_concurrent_disk_reads == 0 || self.write_queue_depth == 0 {
            return bad("max_concurrent_disk_reads and write_queue_depth must be >= 1");
        }
        Ok(())
    }

    /// Byte budget for a level >= 1.
    pub(crate) fn max_bytes_for_level(&self, level: usize) -> u64 {
        let mut b = self.level_base_bytes;
        for _ in 1..level {
            b = b.saturating_mul(self.level_multiplier);
        }
        b
    }
}

/// Per-write settings.
#[derive(Debug, Clone, Copy, Default)]
pub struct WriteOptions {
    /// `fsync` the WAL before the write returns. Without it, a power loss can
    /// drop the last few writes (a process crash can't: they're in the OS).
    pub sync: bool,
}
