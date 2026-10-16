//! Block cache: sharded CLOCK (second-chance) cache with a byte limit.
//!
//! Why not mmap and let the OS cache? The OS page cache can't be capped per
//! process, evicts on its own schedule, and a page fault blocks whatever
//! thread touched the page. Here memory use is a number we choose.
//!
//! Why sharded? One global mutex becomes the bottleneck at high read
//! concurrency. 16 shards (picked by key hash) cut contention ~16x.
//!
//! Why CLOCK instead of strict LRU? Strict LRU must move an entry to the
//! front on every hit, which is a write to shared state on the read path.
//! CLOCK only sets a "referenced" bit on a hit; the eviction sweep gives
//! referenced entries a second chance. Near-LRU hit rates, cheaper hits.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::sstable::block::Block;

const SHARDS: usize = 16;

/// `(table file number, block offset)`. File numbers are never reused, so a
/// stale entry for a deleted table is harmless and just ages out.
pub type CacheKey = (u64, u64);

#[derive(Debug)]
struct Entry {
    block: Block,
    referenced: bool,
}

#[derive(Debug, Default)]
struct Shard {
    map: HashMap<CacheKey, Entry>,
    clock: VecDeque<CacheKey>,
    used: usize,
}

#[derive(Debug)]
pub struct BlockCache {
    shards: Box<[Mutex<Shard>]>,
    cap_per_shard: usize,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

impl BlockCache {
    pub fn new(capacity_bytes: usize) -> Self {
        BlockCache {
            shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect(),
            cap_per_shard: capacity_bytes / SHARDS,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    #[inline]
    fn shard(&self, k: CacheKey) -> &Mutex<Shard> {
        // Cheap mix of the two numbers. Offsets are block-aligned-ish, so
        // multiply to spread the low bits.
        let h = k.0.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ k.1.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
        &self.shards[(h >> 59) as usize % SHARDS]
    }

    pub fn get(&self, k: CacheKey) -> Option<Block> {
        let mut s = self.shard(k).lock();
        match s.map.get_mut(&k) {
            Some(e) => {
                e.referenced = true;
                let b = e.block.clone();
                drop(s);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(b)
            }
            None => {
                drop(s);
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn insert(&self, k: CacheKey, block: Block) {
        let size = block.size();
        if size > self.cap_per_shard {
            return; // Disabled cache, or a block bigger than a whole shard.
        }
        let mut s = self.shard(k).lock();
        if s.map.contains_key(&k) {
            return;
        }
        // Evict until it fits. Each entry gets at most one second chance per
        // sweep, so this ends after at most 2 passes.
        while s.used + size > self.cap_per_shard {
            let Some(victim) = s.clock.pop_front() else { break };
            let referenced = match s.map.get_mut(&victim) {
                Some(e) if e.referenced => {
                    e.referenced = false;
                    true
                }
                Some(_) => false,
                None => continue,
            };
            if referenced {
                s.clock.push_back(victim);
            } else if let Some(e) = s.map.remove(&victim) {
                s.used -= e.block.size();
            }
        }
        s.used += size;
        s.map.insert(k, Entry { block, referenced: false });
        s.clock.push_back(k);
    }

    pub fn used_bytes(&self) -> usize {
        self.shards.iter().map(|s| s.lock().used).sum()
    }

    #[cfg(test)]
    pub fn capacity_bytes(&self) -> usize {
        self.cap_per_shard * SHARDS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::block::BlockBuilder;

    fn blk(n: usize) -> Block {
        let mut b = BlockBuilder::default();
        b.add(&crate::key::encode(&vec![b'x'; n], 1, crate::key::ValueKind::Put), b"");
        let mut out = Vec::new();
        b.finish_into(&mut out);
        Block::new(out.into()).unwrap()
    }

    #[test]
    fn never_exceeds_capacity() {
        let c = BlockCache::new(16 * 4096);
        for i in 0..10_000u64 {
            c.insert((i, i * 4096), blk(1000));
            assert!(c.used_bytes() <= c.capacity_bytes());
        }
    }

    #[test]
    fn hit_after_insert() {
        let c = BlockCache::new(1 << 20);
        c.insert((1, 0), blk(10));
        assert!(c.get((1, 0)).is_some());
        assert!(c.get((1, 4096)).is_none());
    }

    #[test]
    fn zero_capacity_caches_nothing() {
        let c = BlockCache::new(0);
        c.insert((1, 0), blk(10));
        assert!(c.get((1, 0)).is_none());
    }
}
