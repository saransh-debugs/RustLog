//! A `Version` is an immutable snapshot of which SSTables live on which level.
//!
//! Flush and compaction never edit a version in place. They build a new one
//! and swap it in. A reader holding an `Arc<Version>` keeps every table in it
//! alive (and on disk) until it's done, so a compaction can never delete a
//! file out from under a read.

use std::cmp::Ordering;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::iterator::InternalIterator;
use crate::key;
use crate::sstable::{Lookup, ReadMode, Table, TableIter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    pub number: u64,
    pub size: u64,
    /// Smallest and largest internal keys in the file.
    pub smallest: Bytes,
    pub largest: Bytes,
}

impl FileMeta {
    pub fn smallest_user(&self) -> &[u8] {
        key::user_key(&self.smallest)
    }

    pub fn largest_user(&self) -> &[u8] {
        key::user_key(&self.largest)
    }

    pub fn contains_user_key(&self, uk: &[u8]) -> bool {
        self.smallest_user() <= uk && uk <= self.largest_user()
    }

    pub fn overlaps(&self, lo: &[u8], hi: &[u8]) -> bool {
        !(self.largest_user() < lo || self.smallest_user() > hi)
    }
}

#[derive(Debug, Default)]
pub struct Version {
    /// `levels[0]`: newest file first, files may overlap.
    /// `levels[1..]`: sorted by smallest key, never overlap.
    pub levels: Vec<Vec<Arc<Table>>>,
}

impl Version {
    pub fn new(num_levels: usize) -> Self {
        Version { levels: vec![Vec::new(); num_levels] }
    }

    /// Builds the next version: drops `deleted` file numbers, adds `added`.
    pub fn apply(&self, deleted: &[(usize, u64)], added: &[(usize, Arc<Table>)]) -> Result<Version> {
        let mut levels = self.levels.clone();
        for &(lvl, num) in deleted {
            let l = levels.get_mut(lvl).ok_or_else(|| Error::corruption("delete from bad level"))?;
            let before = l.len();
            l.retain(|t| t.number() != num);
            if l.len() == before {
                return Err(Error::corruption(format!("file {num} not on level {lvl}")));
            }
        }
        for (lvl, t) in added {
            levels.get_mut(*lvl).ok_or_else(|| Error::corruption("add to bad level"))?.push(Arc::clone(t));
        }
        let v = Version { levels };
        Ok(v.sorted())
    }

    /// Restores the level invariants after edits.
    pub fn sorted(mut self) -> Self {
        if let Some(l0) = self.levels.first_mut() {
            // Higher file number = flushed later = newer data.
            l0.sort_by_key(|t| std::cmp::Reverse(t.number()));
        }
        for l in self.levels.iter_mut().skip(1) {
            l.sort_by(|a, b| key::compare(&a.meta.smallest, &b.meta.smallest));
        }
        self
    }

    /// Checks that levels >= 1 really don't overlap. Run after every
    /// change in debug builds and from tests.
    pub fn check_invariants(&self) -> Result<()> {
        for (lvl, files) in self.levels.iter().enumerate().skip(1) {
            for w in files.windows(2) {
                if w[0].meta.largest_user() >= w[1].meta.smallest_user() {
                    return Err(Error::corruption(format!(
                        "level {lvl}: files {} and {} overlap",
                        w[0].number(),
                        w[1].number()
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn level_bytes(&self, level: usize) -> u64 {
        self.levels.get(level).map_or(0, |l| l.iter().map(|t| t.meta.size).sum())
    }

    /// Files on `level` whose user-key range overlaps `[lo, hi]`.
    pub fn overlapping(&self, level: usize, lo: &[u8], hi: &[u8]) -> Vec<Arc<Table>> {
        self.levels
            .get(level)
            .map(|l| l.iter().filter(|t| t.meta.overlaps(lo, hi)).cloned().collect())
            .unwrap_or_default()
    }

    /// Point lookup across all levels, newest data first.
    ///
    /// In `CacheOnly` mode a `NeedIo` from any table stops the search right
    /// there. Skipping that table and checking older ones could return a
    /// stale value.
    pub fn get(&self, user_key: &[u8], seek_key: &[u8], mode: ReadMode) -> Result<Lookup> {
        for t in self.levels.first().into_iter().flatten() {
            if !t.meta.contains_user_key(user_key) {
                continue;
            }
            match t.get(user_key, seek_key, mode)? {
                Lookup::NotFound => continue,
                other => return Ok(other),
            }
        }
        for files in self.levels.iter().skip(1) {
            // Non-overlapping and sorted: binary search for the one file
            // that could hold the key.
            let i = files.partition_point(|t| t.meta.largest_user() < user_key);
            let Some(t) = files.get(i) else { continue };
            if t.meta.smallest_user() > user_key {
                continue;
            }
            match t.get(user_key, seek_key, mode)? {
                Lookup::NotFound => continue,
                other => return Ok(other),
            }
        }
        Ok(Lookup::NotFound)
    }

    /// One iterator per L0 file, plus one concatenating iterator per deeper
    /// level. Newest first, as `MergingIterator` expects.
    pub fn iterators(&self, fill_cache: bool) -> Vec<Box<dyn InternalIterator>> {
        let mut out: Vec<Box<dyn InternalIterator>> = Vec::new();
        for t in self.levels.first().into_iter().flatten() {
            out.push(Box::new(t.iter(fill_cache)));
        }
        for files in self.levels.iter().skip(1) {
            if !files.is_empty() {
                out.push(Box::new(LevelIter::new(files.clone(), fill_cache)));
            }
        }
        out
    }

    /// True if no file on a level deeper than `level` overlaps `user_key`.
    /// Then a tombstone at `level` has nothing left to hide and can be dropped.
    pub fn is_bottommost_for(&self, level: usize, user_key: &[u8]) -> bool {
        self.levels.iter().skip(level + 1).all(|files| {
            let i = files.partition_point(|t| t.meta.largest_user() < user_key);
            files.get(i).is_none_or(|t| t.meta.smallest_user() > user_key)
        })
    }
}

/// Walks a sorted, non-overlapping level as one stream, opening one table
/// at a time. A scan over a 100-file level holds one table iterator, not 100.
#[derive(Debug)]
pub struct LevelIter {
    files: Vec<Arc<Table>>,
    idx: usize,
    cur: Option<TableIter>,
    fill_cache: bool,
}

impl LevelIter {
    pub fn new(files: Vec<Arc<Table>>, fill_cache: bool) -> Self {
        LevelIter { files, idx: 0, cur: None, fill_cache }
    }

    fn open(&mut self, idx: usize) {
        self.idx = idx;
        self.cur = self.files.get(idx).map(|t| t.iter(self.fill_cache));
    }

    fn skip_empty(&mut self) -> Result<()> {
        while self.cur.as_ref().is_some_and(|c| !c.valid()) {
            self.open(self.idx + 1);
            if let Some(c) = &mut self.cur {
                c.seek_to_first()?;
            }
        }
        Ok(())
    }
}

impl InternalIterator for LevelIter {
    fn valid(&self) -> bool {
        self.cur.as_ref().is_some_and(|c| c.valid())
    }

    fn key(&self) -> &[u8] {
        self.cur.as_ref().map_or(&[], |c| c.key())
    }

    fn value(&self) -> &[u8] {
        self.cur.as_ref().map_or(&[], |c| c.value())
    }

    fn value_bytes(&self) -> Bytes {
        self.cur.as_ref().map_or_else(Bytes::new, |c| c.value_bytes())
    }

    fn next(&mut self) -> Result<()> {
        if let Some(c) = &mut self.cur {
            c.next()?;
        }
        self.skip_empty()
    }

    fn seek(&mut self, target: &[u8]) -> Result<()> {
        let i = self.files.partition_point(|t| key::compare(&t.meta.largest, target) == Ordering::Less);
        self.open(i);
        if let Some(c) = &mut self.cur {
            c.seek(target)?;
        }
        self.skip_empty()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.open(0);
        if let Some(c) = &mut self.cur {
            c.seek_to_first()?;
        }
        self.skip_empty()
    }
}
