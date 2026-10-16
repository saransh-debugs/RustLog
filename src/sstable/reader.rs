use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use super::block::{Block, BlockIter};
use super::{BlockHandle, BLOCK_TRAILER_LEN, FOOTER_LEN, MAGIC};
use crate::bloom::BloomFilter;
use crate::cache::BlockCache;
use crate::coding::{get_u32, get_u64, masked_crc};
use crate::error::{Error, Result};
use crate::iterator::InternalIterator;
use crate::key::{self, ValueKind};
use crate::stats::{self, Stats};
use crate::version::FileMeta;

/// How a lookup may get blocks it doesn't have cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    /// Never touch the disk. Used on the async task: if everything we need
    /// is cached, the read finishes without a thread hop at all.
    CacheOnly,
    /// May `pread`. Only from our own threads or `spawn_blocking`.
    Io { fill_cache: bool },
}

/// Result of looking one key up in one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    /// This table has no visible version of the key; keep looking.
    NotFound,
    Value(Bytes),
    /// Newest visible version is a tombstone; stop looking, key is gone.
    Deleted,
    /// `CacheOnly` mode needed a block that isn't cached.
    NeedIo,
}

/// An open SSTable. Index and Bloom filter live in memory for the table's
/// lifetime; data blocks come from the cache or a `pread`.
///
/// Why `pread` and not mmap? With mmap, a disk error is a SIGBUS that kills
/// the process, memory use is whatever the kernel decides, and you can't
/// tell in advance which access will block on a page fault. `pread` returns
/// `io::Error`, reads exactly what we ask, and we know exactly where we block.
#[derive(Debug)]
pub struct Table {
    pub meta: FileMeta,
    file: File,
    path: PathBuf,
    index: Block,
    filter: BloomFilter,
    ctx: TableCtx,
    /// Set once a compaction has replaced this table in the MANIFEST.
    obsolete: AtomicBool,
}

/// What every open table shares with the engine.
#[derive(Debug, Clone)]
pub struct TableCtx {
    pub cache: Arc<BlockCache>,
    pub stats: Arc<Stats>,
    /// Where to send our path for deletion when the last reader lets go.
    pub deleter: Option<crossbeam_channel::Sender<PathBuf>>,
    /// BENCHMARK ONLY: make every block read go to the device.
    pub bypass_os_cache: bool,
    /// BENCHMARK ONLY: extra sleep per block read, to model a slower device.
    pub simulated_read_latency: std::time::Duration,
}

impl TableCtx {
    #[cfg(test)]
    pub fn for_test(cache_bytes: usize) -> Self {
        TableCtx {
            cache: Arc::new(BlockCache::new(cache_bytes)),
            stats: Arc::new(Stats::default()),
            deleter: None,
            bypass_os_cache: false,
            simulated_read_latency: std::time::Duration::ZERO,
        }
    }
}

impl Table {
    /// Opens and validates a table. Blocking.
    pub fn open(path: PathBuf, meta: FileMeta, ctx: &TableCtx) -> Result<Table> {
        let file = File::open(&path)?;
        if ctx.bypass_os_cache {
            os_cache::disable(&file)?;
        }
        let size = file.metadata()?.len();
        if size != meta.size {
            return Err(Error::corruption(format!(
                "{}: size {} but MANIFEST says {}",
                path.display(),
                size,
                meta.size
            )));
        }
        if size < FOOTER_LEN as u64 {
            return Err(Error::corruption(format!("{}: too small to be an SSTable", path.display())));
        }
        let mut footer = [0u8; FOOTER_LEN];
        file.read_exact_at(&mut footer, size - FOOTER_LEN as u64)?;
        let bad = || Error::corruption(format!("{}: bad footer", path.display()));
        if get_u64(&footer, 32).ok_or_else(bad)? != MAGIC {
            return Err(bad());
        }
        let fh = BlockHandle::decode(&footer[0..16]).ok_or_else(bad)?;
        let ih = BlockHandle::decode(&footer[16..32]).ok_or_else(bad)?;
        let filter_bytes = read_checked(&file, fh, size, false)?;
        let index = Block::new(read_checked(&file, ih, size, false)?)?;
        Ok(Table {
            meta,
            file,
            path,
            index,
            filter: BloomFilter::new(filter_bytes),
            ctx: ctx.clone(),
            obsolete: AtomicBool::new(false),
        })
    }

    pub fn number(&self) -> u64 {
        self.meta.number
    }

    /// Marks the table for deletion once nobody holds it any more.
    pub fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::Release);
    }

    fn read_block(&self, h: BlockHandle, mode: ReadMode) -> Result<Option<Block>> {
        let ck = (self.meta.number, h.offset);
        if let Some(b) = self.ctx.cache.get(ck) {
            return Ok(Some(b));
        }
        let ReadMode::Io { fill_cache } = mode else {
            return Ok(None);
        };
        stats::add(&self.ctx.stats.disk_block_reads, 1);
        let block = Block::new(read_checked(&self.file, h, self.meta.size, self.ctx.bypass_os_cache)?)?;
        if !self.ctx.simulated_read_latency.is_zero() {
            // Same effect on the calling thread as a slow `pread`: it sleeps.
            std::thread::sleep(self.ctx.simulated_read_latency);
        }
        if fill_cache {
            self.ctx.cache.insert(ck, block.clone());
        }
        Ok(Some(block))
    }

    /// Finds the newest version of `user_key` with seq <= the seq inside
    /// `seek_key` (see `key::seek_key`).
    pub fn get(&self, user_key: &[u8], seek_key: &[u8], mode: ReadMode) -> Result<Lookup> {
        stats::add(&self.ctx.stats.bloom_checks, 1);
        if !self.filter.may_contain(user_key) {
            stats::add(&self.ctx.stats.bloom_negatives, 1);
            return Ok(Lookup::NotFound);
        }
        let mut idx = self.index.iter();
        idx.seek(seek_key)?;
        if !idx.valid() {
            return Ok(Lookup::NotFound);
        }
        let h = BlockHandle::decode(idx.value()).ok_or_else(|| Error::corruption("bad index entry"))?;
        let Some(block) = self.read_block(h, mode)? else {
            return Ok(Lookup::NeedIo);
        };
        let mut it = block.iter();
        it.seek(seek_key)?;
        if !it.valid() || key::user_key(it.key()) != user_key {
            return Ok(Lookup::NotFound);
        }
        match key::seq_kind(it.key()) {
            Some((_, ValueKind::Put)) => Ok(Lookup::Value(it.value_bytes())),
            Some((_, ValueKind::Delete)) => Ok(Lookup::Deleted),
            None => Err(Error::corruption("bad key trailer")),
        }
    }

    pub fn iter(self: &Arc<Self>, fill_cache: bool) -> TableIter {
        TableIter { table: Arc::clone(self), index: self.index.iter(), data: None, fill_cache }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        if !self.obsolete.load(Ordering::Acquire) {
            return;
        }
        // Hand the unlink to a background thread: the last reference may be
        // dropped on a Tokio worker, and `unlink` is a blocking syscall.
        // If the channel is gone (shutdown), the next `open` cleans up
        // orphans, so ignoring the error loses nothing.
        if let Some(d) = &self.ctx.deleter {
            let _ = d.send(self.path.clone());
        }
    }
}

/// Reads a block plus its CRC trailer and checks it. Blocking.
fn read_checked(file: &File, h: BlockHandle, file_size: u64, drop_from_os_cache: bool) -> Result<Bytes> {
    let total = h
        .len
        .checked_add(BLOCK_TRAILER_LEN as u64)
        .filter(|&t| h.offset.checked_add(t).is_some_and(|end| end <= file_size))
        .ok_or_else(|| Error::corruption("block handle out of range"))?;
    // Read straight into a BytesMut, then freeze: one allocation, and the
    // block can be shared (cache + iterators) without copying.
    let mut buf = BytesMut::zeroed(total as usize);
    file.read_exact_at(&mut buf, h.offset)?;
    if drop_from_os_cache {
        os_cache::evict(file, h.offset, total);
    }
    let data_len = h.len as usize;
    let stored = get_u32(&buf, data_len).ok_or_else(|| Error::corruption("missing block crc"))?;
    if masked_crc(&buf[..data_len]) != stored {
        return Err(Error::corruption(format!("block checksum mismatch at offset {}", h.offset)));
    }
    buf.truncate(data_len);
    Ok(buf.freeze())
}

/// Benchmark-only helpers to take the OS page cache out of the picture, so a
/// "disk read" in a benchmark really waits on the device.
mod os_cache {
    use std::fs::File;

    /// macOS: `F_NOCACHE` makes reads on this fd skip the unified buffer cache.
    #[cfg(target_os = "macos")]
    pub fn disable(f: &File) -> std::io::Result<()> {
        use std::os::fd::AsRawFd;
        // SAFETY: plain fcntl on an fd we own; no pointers involved.
        let r = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_NOCACHE, 1) };
        if r == -1 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    pub fn disable(_f: &File) -> std::io::Result<()> {
        Ok(())
    }

    /// Linux: O_DIRECT needs aligned buffers and offsets, which our block
    /// layout doesn't have. Instead, evict the range right after reading it.
    #[cfg(target_os = "linux")]
    pub fn evict(f: &File, off: u64, len: u64) {
        use std::os::fd::AsRawFd;
        // SAFETY: advisory call on an fd we own; failure is harmless.
        unsafe {
            libc::posix_fadvise(f.as_raw_fd(), off as libc::off_t, len as libc::off_t, libc::POSIX_FADV_DONTNEED);
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn evict(_f: &File, _off: u64, _len: u64) {}
}

/// Two-level iterator: walk the index, load each data block, walk it.
#[derive(Debug)]
pub struct TableIter {
    table: Arc<Table>,
    index: BlockIter,
    data: Option<BlockIter>,
    /// Compaction reads every block once; caching them would just evict
    /// the hot blocks that user reads need.
    fill_cache: bool,
}

impl TableIter {
    fn load_block(&mut self) -> Result<()> {
        if !self.index.valid() {
            self.data = None;
            return Ok(());
        }
        let h = BlockHandle::decode(self.index.value()).ok_or_else(|| Error::corruption("bad index entry"))?;
        let b = self
            .table
            .read_block(h, ReadMode::Io { fill_cache: self.fill_cache })?
            .ok_or_else(|| Error::corruption("io read returned nothing"))?;
        self.data = Some(b.iter());
        Ok(())
    }

    /// Skips past exhausted data blocks.
    fn skip_forward(&mut self) -> Result<()> {
        while self.data.as_ref().is_some_and(|d| !d.valid()) {
            self.index.next()?;
            self.load_block()?;
            if let Some(d) = &mut self.data {
                d.seek_to_first()?;
            }
        }
        Ok(())
    }
}

impl InternalIterator for TableIter {
    fn valid(&self) -> bool {
        self.data.as_ref().is_some_and(|d| d.valid())
    }

    fn key(&self) -> &[u8] {
        self.data.as_ref().map_or(&[], |d| d.key())
    }

    fn value(&self) -> &[u8] {
        self.data.as_ref().map_or(&[], |d| d.value())
    }

    fn value_bytes(&self) -> Bytes {
        self.data.as_ref().map_or_else(Bytes::new, |d| d.value_bytes())
    }

    fn next(&mut self) -> Result<()> {
        if let Some(d) = &mut self.data {
            d.next()?;
        }
        self.skip_forward()
    }

    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.index.seek(target)?;
        self.load_block()?;
        if let Some(d) = &mut self.data {
            d.seek(target)?;
        }
        self.skip_forward()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.index.seek_to_first()?;
        self.load_block()?;
        if let Some(d) = &mut self.data {
            d.seek_to_first()?;
        }
        self.skip_forward()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::encode;
    use crate::sstable::TableBuilder;

    fn make(dir: &std::path::Path, n: usize) -> Arc<Table> {
        let path = dir.join("000001.sst");
        let mut b = TableBuilder::create(&path, 4096, 10, None).unwrap();
        for i in 0..n {
            let k = encode(format!("key{i:08}").as_bytes(), (i + 1) as u64, ValueKind::Put);
            b.add(&k, format!("value{i}").as_bytes()).unwrap();
        }
        let built = b.finish().unwrap();
        let meta = FileMeta { number: 1, size: built.size, smallest: built.smallest, largest: built.largest };
        Arc::new(Table::open(path, meta, &TableCtx::for_test(8 << 20)).unwrap())
    }

    #[test]
    fn million_keys_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let n = 1_000_000;
        let t = make(dir.path(), n);
        let mode = ReadMode::Io { fill_cache: true };
        for i in (0..n).step_by(7) {
            let uk = format!("key{i:08}");
            let got = t.get(uk.as_bytes(), &key::seek_key(uk.as_bytes(), u64::MAX >> 8), mode).unwrap();
            assert_eq!(got, Lookup::Value(Bytes::from(format!("value{i}"))));
        }
        // Missing keys: mostly skipped by the bloom filter.
        let before = t.ctx.stats.bloom_negatives.load(Ordering::Relaxed);
        for i in 0..10_000 {
            let uk = format!("nope{i:08}");
            let got = t.get(uk.as_bytes(), &key::seek_key(uk.as_bytes(), 1 << 40), mode).unwrap();
            assert_eq!(got, Lookup::NotFound);
        }
        let skipped = t.ctx.stats.bloom_negatives.load(Ordering::Relaxed) - before;
        assert!(skipped > 9_800, "bloom skipped only {skipped}/10000");

        let mut it = t.iter(false);
        it.seek_to_first().unwrap();
        let mut count = 0;
        while it.valid() {
            count += 1;
            it.next().unwrap();
        }
        assert_eq!(count, n);
    }

    #[test]
    fn snapshot_hides_newer_versions() {
        let dir = tempfile::tempdir().unwrap();
        let t = make(dir.path(), 100);
        // key00000050 was written at seq 51.
        let uk = b"key00000050";
        let mode = ReadMode::Io { fill_cache: true };
        assert_eq!(t.get(uk, &key::seek_key(uk, 50), mode).unwrap(), Lookup::NotFound);
        assert!(matches!(t.get(uk, &key::seek_key(uk, 51), mode).unwrap(), Lookup::Value(_)));
    }

    #[test]
    fn cache_only_mode_never_reads_disk() {
        let dir = tempfile::tempdir().unwrap();
        let t = make(dir.path(), 1000);
        let uk = b"key00000500";
        let sk = key::seek_key(uk, 1 << 40);
        assert_eq!(t.get(uk, &sk, ReadMode::CacheOnly).unwrap(), Lookup::NeedIo);
        assert!(matches!(t.get(uk, &sk, ReadMode::Io { fill_cache: true }).unwrap(), Lookup::Value(_)));
        assert!(matches!(t.get(uk, &sk, ReadMode::CacheOnly).unwrap(), Lookup::Value(_)));
    }

    #[test]
    fn every_flipped_byte_is_caught() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("000001.sst");
        let mut b = TableBuilder::create(&path, 256, 10, None).unwrap();
        for i in 0..50 {
            b.add(&encode(format!("k{i:03}").as_bytes(), 1, ValueKind::Put), b"v").unwrap();
        }
        let built = b.finish().unwrap();
        let orig = std::fs::read(&path).unwrap();
        for pos in 0..orig.len() {
            let mut bad = orig.clone();
            bad[pos] ^= 0x40;
            std::fs::write(&path, &bad).unwrap();
            let meta = FileMeta {
                number: 1,
                size: built.size,
                smallest: built.smallest.clone(),
                largest: built.largest.clone(),
            };
            let res = Table::open(path.clone(), meta, &TableCtx::for_test(0));
            let Ok(t) = res else { continue };
            let t = Arc::new(t);
            // Every key must be either correct or an error. Never a wrong value.
            let mut it = t.iter(false);
            let mut r = it.seek_to_first();
            let mut seen = 0;
            while r.is_ok() && it.valid() {
                assert_eq!(it.value(), b"v", "flip at {pos} produced a wrong value");
                seen += 1;
                r = it.next();
            }
            // A flipped bit in the filter can only cause false "maybe"s, or
            // false "no"s. A false "no" would be silent data loss, so the
            // filter is checksummed too: if we got here, it was intact,
            // or the flip was in a part that doesn't change the answer.
            if r.is_ok() {
                assert_eq!(seen, 50, "flip at {pos} silently dropped keys");
            }
        }
    }
}
