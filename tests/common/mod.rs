#![allow(dead_code)]

use rustlog::Options;

/// Tiny sizes so a few thousand writes exercise flush, every compaction
/// path, and write stalls.
pub fn small_opts() -> Options {
    Options {
        memtable_size: 16 << 10,
        block_size: 512,
        target_file_size: 16 << 10,
        level_base_bytes: 64 << 10,
        level_multiplier: 4,
        l0_compaction_trigger: 2,
        l0_slowdown_trigger: 4,
        l0_stop_trigger: 6,
        block_cache_bytes: 256 << 10,
        num_levels: 5,
        ..Options::default()
    }
}

pub fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().expect("runtime")
}
