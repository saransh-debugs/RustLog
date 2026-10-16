//! RustLog: a disk-backed LSM-tree storage engine.
//!
//! ```no_run
//! # async fn demo() -> rustlog::Result<()> {
//! let db = rustlog::Db::open("/tmp/mydb", rustlog::Options::default()).await?;
//! db.put("hello", "world").await?;
//! assert_eq!(db.get(b"hello").await?.as_deref(), Some(&b"world"[..]));
//! db.close().await?;
//! # Ok(()) }
//! ```
//!
//! Writes go to a write-ahead log and an in-memory skip list. Full memtables
//! are flushed to sorted SSTable files, which a background thread merges with
//! leveled compaction. Reads check memory first, then the SSTables, using a
//! Bloom filter per table to skip files that can't hold the key.
//!
//! No disk I/O ever runs on a Tokio worker thread.

#[cfg(not(unix))]
compile_error!("RustLog uses positional reads (pread) and only supports Unix.");

mod batch;
mod bloom;
mod cache;
mod coding;
mod compaction;
mod engine;
mod error;
mod flush;
mod io;
mod iterator;
mod key;
mod manifest;
mod memtable;
mod options;
mod sstable;
mod stats;
mod version;
mod wal;
mod writer;

pub use batch::WriteBatch;
pub use engine::Db;
pub use error::{Error, Result};
pub use io::{FailPoint, FailPoints};
pub use options::{Options, WriteOptions};
pub use stats::StatsSnapshot;
