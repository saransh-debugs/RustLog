use bytes::Bytes;

use crate::key::ValueKind;

/// A group of writes applied atomically: after a crash, either all of them
/// are there or none are (they share one checksummed WAL record).
#[derive(Debug, Clone, Default)]
pub struct WriteBatch {
    pub(crate) ops: Vec<(ValueKind, Bytes, Bytes)>,
    pub(crate) bytes: usize,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> &mut Self {
        let (k, v) = (key.into(), value.into());
        self.bytes += k.len() + v.len();
        self.ops.push((ValueKind::Put, k, v));
        self
    }

    pub fn delete(&mut self, key: impl Into<Bytes>) -> &mut Self {
        let k = key.into();
        self.bytes += k.len();
        self.ops.push((ValueKind::Delete, k, Bytes::new()));
        self
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Key + value bytes.
    pub fn size_bytes(&self) -> usize {
        self.bytes
    }
}
