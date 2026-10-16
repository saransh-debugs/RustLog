//! Sorted String Table: an immutable, sorted file of internal keys.
//!
//! ```text
//! [data block][crc] ... [data block][crc]
//! [bloom filter][crc]
//! [index block][crc]          one entry per data block: last key -> (offset, len)
//! [footer: 40 bytes]          filter (off, len), index (off, len), magic
//! ```
//!
//! Every block carries a CRC32C. A flipped bit becomes `Error::Corruption`,
//! never a wrong answer.

pub mod block;
pub mod builder;
pub mod reader;

pub use builder::TableBuilder;
pub use reader::{Lookup, ReadMode, Table, TableIter};

pub const FOOTER_LEN: usize = 40;
pub const MAGIC: u64 = 0x5275_7374_4c6f_6731; // "RustLog1"
/// CRC trailer after every block.
pub const BLOCK_TRAILER_LEN: usize = 4;

/// Where a block lives in the file. `len` excludes the CRC trailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: u64,
    pub len: u64,
}

impl BlockHandle {
    pub fn encode(&self, out: &mut Vec<u8>) {
        crate::coding::put_u64(out, self.offset);
        crate::coding::put_u64(out, self.len);
    }

    pub fn decode(b: &[u8]) -> Option<BlockHandle> {
        Some(BlockHandle { offset: crate::coding::get_u64(b, 0)?, len: crate::coding::get_u64(b, 8)? })
    }
}
