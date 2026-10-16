//! In-memory write buffer: a lock-free skip list of internal keys.
//!
//! Why a skip list and not `RwLock<BTreeMap>`? With a lock, every reader
//! contends with the writer on one cache line, and a writer holding the
//! write lock stalls every reader. `crossbeam_skiplist::SkipMap` lets reads
//! run fully in parallel with the (single) writer thread: no locks, just
//! atomic pointer loads. Memory is reclaimed with epoch-based GC.

use std::ops::Bound;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::error::Result;
use crate::iterator::InternalIterator;
use crate::key::{self, InternalKey, KeyRef, ValueKind};
use crate::sstable::Lookup;

/// Rough per-entry overhead of a skip list node (tower pointers, refcount,
/// two `Bytes` headers). Counted so `memtable_size` tracks real RAM, not
/// just payload bytes. Otherwise 1M tiny keys would blow way past the cap.
const NODE_OVERHEAD: usize = 80;

#[derive(Debug)]
pub struct MemTable {
    map: SkipMap<InternalKey, Bytes>,
    size: AtomicUsize,
    max_seq: AtomicU64,
    /// The WAL file holding this memtable's writes. Deleted after flush.
    pub wal_number: u64,
}

impl MemTable {
    pub fn new(wal_number: u64) -> Self {
        MemTable { map: SkipMap::new(), size: AtomicUsize::new(0), max_seq: AtomicU64::new(0), wal_number }
    }

    /// Only the writer thread (or recovery) calls this, so seqs arrive in
    /// increasing order.
    pub fn insert(&self, seq: u64, kind: ValueKind, user_key: &[u8], value: Bytes) {
        let k = Bytes::from(key::encode(user_key, seq, kind));
        let sz = k.len() + value.len() + NODE_OVERHEAD;
        self.map.insert(InternalKey(k), value);
        self.size.fetch_add(sz, Ordering::Relaxed);
        self.max_seq.fetch_max(seq, Ordering::Relaxed);
    }

    /// Newest version of `user_key` with seq <= `seq`.
    /// `None` = this memtable doesn't know the key; keep looking.
    pub fn get(&self, user_key: &[u8], seq: u64) -> Option<Lookup> {
        let sk = key::seek_key(user_key, seq);
        // Search with a borrowed key: no allocation on the read path.
        let e = self.map.lower_bound(Bound::Included(KeyRef::new(&sk)))?;
        let ik = &e.key().0;
        if key::user_key(ik) != user_key {
            return None;
        }
        match key::seq_kind(ik) {
            Some((_, ValueKind::Put)) => Some(Lookup::Value(e.value().clone())),
            Some((_, ValueKind::Delete)) => Some(Lookup::Deleted),
            None => None,
        }
    }

    pub fn approximate_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn max_seq(&self) -> u64 {
        self.max_seq.load(Ordering::Relaxed)
    }

    pub fn iter(self: &Arc<Self>) -> MemTableIter {
        MemTableIter { mem: Arc::clone(self), cur: None }
    }
}

/// Cursor over a memtable.
///
/// A crossbeam `Entry` borrows the map, which would make this iterator
/// self-referential. Instead we remember the current key (a `Bytes`, so a
/// refcount bump) and re-seek past it on `next()`. That's O(log n) per step
/// instead of O(1), which is fine: memtable scans are short next to disk I/O.
#[derive(Debug)]
pub struct MemTableIter {
    mem: Arc<MemTable>,
    cur: Option<(Bytes, Bytes)>,
}

impl MemTableIter {
    fn set(&mut self, bound: Bound<&KeyRef>) {
        self.cur = self.mem.map.lower_bound(bound).map(|e| (e.key().0.clone(), e.value().clone()));
    }
}

impl InternalIterator for MemTableIter {
    fn valid(&self) -> bool {
        self.cur.is_some()
    }

    fn key(&self) -> &[u8] {
        self.cur.as_ref().map_or(&[], |(k, _)| k)
    }

    fn value(&self) -> &[u8] {
        self.cur.as_ref().map_or(&[], |(_, v)| v)
    }

    fn value_bytes(&self) -> Bytes {
        self.cur.as_ref().map_or_else(Bytes::new, |(_, v)| v.clone())
    }

    fn next(&mut self) -> Result<()> {
        if let Some((k, _)) = self.cur.take() {
            self.set(Bound::Excluded(KeyRef::new(&k)));
        }
        Ok(())
    }

    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.set(Bound::Included(KeyRef::new(target)));
        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.set(Bound::Unbounded);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_respects_snapshot_and_tombstones() {
        let m = MemTable::new(1);
        m.insert(1, ValueKind::Put, b"a", Bytes::from_static(b"1"));
        m.insert(2, ValueKind::Put, b"a", Bytes::from_static(b"2"));
        m.insert(3, ValueKind::Delete, b"a", Bytes::new());
        assert_eq!(m.get(b"a", 1), Some(Lookup::Value(Bytes::from_static(b"1"))));
        assert_eq!(m.get(b"a", 2), Some(Lookup::Value(Bytes::from_static(b"2"))));
        assert_eq!(m.get(b"a", 3), Some(Lookup::Deleted));
        assert_eq!(m.get(b"a", 0), None);
        assert_eq!(m.get(b"b", 10), None);
        assert_eq!(m.get(b"", 10), None);
    }

    #[test]
    fn iter_is_sorted() {
        let m = Arc::new(MemTable::new(1));
        for (i, k) in ["d", "b", "a", "c"].iter().enumerate() {
            m.insert(i as u64 + 1, ValueKind::Put, k.as_bytes(), Bytes::new());
        }
        let mut it = m.iter();
        it.seek_to_first().unwrap();
        let mut keys = Vec::new();
        while it.valid() {
            keys.push(key::user_key(it.key()).to_vec());
            it.next().unwrap();
        }
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
    }
}
