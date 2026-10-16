use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;

use super::block::BlockBuilder;
use super::{BlockHandle, MAGIC};
use crate::bloom::BloomBuilder;
use crate::coding::{masked_crc, put_u32, put_u64};
use crate::error::Result;
use crate::io::{self, FailPoint, FailPoints};
use crate::key;

/// What a finished table looks like, for the MANIFEST.
#[derive(Debug, Clone)]
pub struct BuiltTable {
    pub size: u64,
    pub smallest: Bytes,
    pub largest: Bytes,
}

/// Streams sorted entries into a new SSTable file.
///
/// Only ever runs on the flush or compaction thread: every `write` can block
/// on the disk, which is fine there and never fine on a Tokio worker.
#[derive(Debug)]
pub struct TableBuilder {
    file: BufWriter<File>,
    offset: u64,
    block_size: usize,
    bloom_bits: usize,
    data: BlockBuilder,
    index: BlockBuilder,
    bloom: BloomBuilder,
    /// Reused per block, so a big flush doesn't allocate per block.
    scratch: Vec<u8>,
    smallest: Vec<u8>,
    last_user_key: Vec<u8>,
    index_key: Vec<u8>,
    entries: u64,
    fail: Option<Arc<FailPoints>>,
}

impl TableBuilder {
    pub fn create(path: &Path, block_size: usize, bloom_bits: usize, fail: Option<Arc<FailPoints>>) -> Result<Self> {
        let f = File::options().write(true).create_new(true).open(path)?;
        Ok(TableBuilder {
            // 256 KiB buffer: SST writes are sequential, so batch them into
            // few large syscalls.
            file: BufWriter::with_capacity(256 << 10, f),
            offset: 0,
            block_size,
            bloom_bits,
            data: BlockBuilder::default(),
            index: BlockBuilder::default(),
            bloom: BloomBuilder::default(),
            scratch: Vec::with_capacity(block_size + 1024),
            smallest: Vec::new(),
            last_user_key: Vec::new(),
            index_key: Vec::new(),
            entries: 0,
            fail,
        })
    }

    /// Entries must come in sorted internal-key order.
    pub fn add(&mut self, ikey: &[u8], value: &[u8]) -> Result<()> {
        if self.entries == 0 {
            self.smallest.extend_from_slice(ikey);
        }
        let uk = key::user_key(ikey);
        // Bloom is on user keys; add each once even if it has many versions.
        if self.entries == 0 || uk != self.last_user_key.as_slice() {
            if self.bloom_bits > 0 {
                self.bloom.add(uk);
            }
            self.last_user_key.clear();
            self.last_user_key.extend_from_slice(uk);
        }
        self.data.add(ikey, value);
        self.entries += 1;
        if self.data.estimated_size() >= self.block_size {
            self.flush_data_block()?;
        }
        Ok(())
    }

    /// Bytes written so far plus the open block. Used to cut output files.
    pub fn estimated_size(&self) -> u64 {
        self.offset + self.data.estimated_size() as u64
    }

    fn write_raw_block(&mut self) -> Result<BlockHandle> {
        io::check(&self.fail, FailPoint::SstWrite)?;
        let crc = masked_crc(&self.scratch);
        let handle = BlockHandle { offset: self.offset, len: self.scratch.len() as u64 };
        put_u32(&mut self.scratch, crc);
        self.file.write_all(&self.scratch)?;
        self.offset += self.scratch.len() as u64;
        self.scratch.clear();
        Ok(handle)
    }

    fn flush_data_block(&mut self) -> Result<()> {
        if self.data.is_empty() {
            return Ok(());
        }
        // Index key = the block's last key. A seek for X lands on the first
        // block whose last key >= X, which is the only block X can be in.
        self.index_key.clear();
        self.index_key.extend_from_slice(self.data.last_key());
        self.data.finish_into(&mut self.scratch);
        let h = self.write_raw_block()?;
        let mut hv = [0u8; 16];
        hv[..8].copy_from_slice(&h.offset.to_le_bytes());
        hv[8..].copy_from_slice(&h.len.to_le_bytes());
        self.index.add(&self.index_key, &hv);
        Ok(())
    }

    /// Writes filter, index, footer, then fsyncs. Returns the table's shape.
    pub fn finish(mut self) -> Result<BuiltTable> {
        if self.entries == 0 {
            return Err(crate::Error::InvalidArgument("refusing to write an empty SSTable".into()));
        }
        let largest = Bytes::copy_from_slice(self.data_last_or_index_last());
        self.flush_data_block()?;

        let filter =
            if self.bloom_bits > 0 && !self.bloom.is_empty() { self.bloom.finish(self.bloom_bits) } else { Vec::new() };
        self.scratch.extend_from_slice(&filter);
        let fh = self.write_raw_block()?;

        self.index.finish_into(&mut self.scratch);
        let ih = self.write_raw_block()?;

        let mut footer = Vec::with_capacity(super::FOOTER_LEN);
        fh.encode(&mut footer);
        ih.encode(&mut footer);
        put_u64(&mut footer, MAGIC);
        self.file.write_all(&footer)?;
        self.offset += footer.len() as u64;

        io::check(&self.fail, FailPoint::SstWrite)?;
        let f = self.file.into_inner().map_err(|e| e.into_error())?;
        // fsync before the MANIFEST points at this file. Otherwise a crash
        // could leave a MANIFEST that references a file full of zeros.
        f.sync_all()?;
        Ok(BuiltTable { size: self.offset, smallest: Bytes::from(self.smallest), largest })
    }

    fn data_last_or_index_last(&self) -> &[u8] {
        if self.data.is_empty() {
            self.index.last_key()
        } else {
            self.data.last_key()
        }
    }
}
