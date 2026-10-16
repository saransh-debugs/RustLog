//! Internal keys: `user_key ++ trailer`, where `trailer = (seq << 8) | kind`
//! stored as 8 big-endian bytes.
//!
//! Sort order: user key ascending, then sequence number DESCENDING.
//! So the newest version of a key comes first, and a reader at snapshot `S`
//! can seek to `(key, S)` and land on the newest version it may see.
//!
//! We can't compare the encoded bytes directly: if `"a"` is a prefix of
//! `"ab"`, a plain byte compare would compare `a`'s trailer with `b`.
//! So every compare splits off the last 8 bytes first.

use std::borrow::Borrow;
use std::cmp::Ordering;

use bytes::Bytes;

pub const TRAILER_LEN: usize = 8;
/// Sequence numbers use 56 bits; the low 8 bits of the trailer hold the kind.
pub const MAX_SEQ: u64 = (1 << 56) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum ValueKind {
    /// Tombstone: "this key was deleted at this seq".
    Delete = 0,
    Put = 1,
}

impl ValueKind {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(ValueKind::Delete),
            1 => Some(ValueKind::Put),
            _ => None,
        }
    }
}

/// The highest kind value. A seek key uses it so that it sorts before every
/// real entry with the same `(user_key, seq)`.
pub const SEEK_KIND: ValueKind = ValueKind::Put;

pub fn trailer(seq: u64, kind: ValueKind) -> u64 {
    debug_assert!(seq <= MAX_SEQ);
    (seq << 8) | kind as u64
}

pub fn encode_into(buf: &mut Vec<u8>, user_key: &[u8], seq: u64, kind: ValueKind) {
    buf.extend_from_slice(user_key);
    buf.extend_from_slice(&trailer(seq, kind).to_be_bytes());
}

pub fn encode(user_key: &[u8], seq: u64, kind: ValueKind) -> Vec<u8> {
    let mut v = Vec::with_capacity(user_key.len() + TRAILER_LEN);
    encode_into(&mut v, user_key, seq, kind);
    v
}

/// Splits an internal key. Returns `None` if it's too short to have a trailer.
#[inline]
pub fn split(ikey: &[u8]) -> Option<(&[u8], u64)> {
    let cut = ikey.len().checked_sub(TRAILER_LEN)?;
    let (user, t) = ikey.split_at(cut);
    let t = u64::from_be_bytes(t.try_into().ok()?);
    Some((user, t))
}

#[inline]
pub fn user_key(ikey: &[u8]) -> &[u8] {
    &ikey[..ikey.len().saturating_sub(TRAILER_LEN)]
}

/// Returns `(seq, kind)`. Malformed keys come back as `None`.
pub fn seq_kind(ikey: &[u8]) -> Option<(u64, ValueKind)> {
    let (_, t) = split(ikey)?;
    Some((t >> 8, ValueKind::from_u8((t & 0xff) as u8)?))
}

/// The one comparator used everywhere: memtable, blocks, index, merge.
#[inline]
pub fn compare(a: &[u8], b: &[u8]) -> Ordering {
    match (split(a), split(b)) {
        (Some((ua, ta)), Some((ub, tb))) => ua.cmp(ub).then_with(|| tb.cmp(&ta)),
        // Malformed keys never get written, but compare must stay total.
        _ => a.cmp(b),
    }
}

/// An encoded internal key as an unsized type, so the skip list can be
/// searched with a borrowed `&KeyRef` instead of building an owned key
/// (and allocating) on every lookup. Same trick as `Path` / `str`.
#[derive(Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct KeyRef([u8]);

impl KeyRef {
    pub fn new(b: &[u8]) -> &KeyRef {
        // SAFETY: KeyRef is #[repr(transparent)] over [u8], so the two
        // pointer types have the same layout and metadata.
        unsafe { &*(b as *const [u8] as *const KeyRef) }
    }
}

impl Ord for KeyRef {
    fn cmp(&self, other: &Self) -> Ordering {
        compare(&self.0, &other.0)
    }
}

impl PartialOrd for KeyRef {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// An owned internal key. `Bytes` so clones are a refcount bump.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey(pub Bytes);

impl Borrow<KeyRef> for InternalKey {
    fn borrow(&self) -> &KeyRef {
        KeyRef::new(&self.0)
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        compare(&self.0, &other.0)
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Seek key for "newest version of `user_key` visible at `seq`", built on the
/// stack for normal-sized keys.
pub type SeekBuf = smallvec::SmallVec<[u8; 64]>;

pub fn seek_key(user_key: &[u8], seq: u64) -> SeekBuf {
    let mut b = SeekBuf::with_capacity(user_key.len() + TRAILER_LEN);
    b.extend_from_slice(user_key);
    b.extend_from_slice(&trailer(seq, SEEK_KIND).to_be_bytes());
    b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_sorts_first() {
        let a = encode(b"k", 10, ValueKind::Put);
        let b = encode(b"k", 5, ValueKind::Put);
        assert_eq!(compare(&a, &b), Ordering::Less);
    }

    #[test]
    fn prefix_keys_order_by_user_key() {
        let a = encode(b"a", 1, ValueKind::Put);
        let ab = encode(b"ab", 100, ValueKind::Put);
        assert_eq!(compare(&a, &ab), Ordering::Less);
    }

    #[test]
    fn seek_key_lands_on_visible_version() {
        let newer = encode(b"k", 11, ValueKind::Put);
        let visible = encode(b"k", 10, ValueKind::Delete);
        let seek = seek_key(b"k", 10);
        assert_eq!(compare(&newer, &seek), Ordering::Less);
        assert_eq!(compare(&seek, &visible), Ordering::Less);
    }

    #[test]
    fn split_roundtrip() {
        let k = encode(b"hello", 42, ValueKind::Delete);
        assert_eq!(user_key(&k), b"hello");
        assert_eq!(seq_kind(&k), Some((42, ValueKind::Delete)));
        assert!(split(b"short").is_none());
    }
}
