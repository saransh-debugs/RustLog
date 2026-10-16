use std::sync::Arc;

/// Every fallible call in RustLog returns this. No `unwrap()` anywhere in `src/`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A checksum failed or a file has a shape we never write.
    #[error("corruption: {0}")]
    Corruption(String),

    /// A background write (WAL fsync, flush, compaction) failed earlier.
    /// The DB stays open for reads but refuses writes.
    ///
    /// Why not retry? After a failed `fsync`, Linux may drop the dirty pages
    /// and report success on the next `fsync` ("fsyncgate", PostgreSQL 2018).
    /// A retry can silently lose data. Going read-only is the only safe move.
    #[error("database is read-only after a background error: {0}")]
    ReadOnly(Arc<str>),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Another process (or another `Db` in this process) holds the LOCK file.
    #[error("database directory is locked by another instance")]
    Locked,

    #[error("database is closed")]
    Closed,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub(crate) fn corruption(msg: impl Into<String>) -> Self {
        Error::Corruption(msg.into())
    }
}
