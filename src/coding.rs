//! Byte encoding helpers: varints and fixed-width little-endian integers.
//!
//! Every decoder returns `None` on short input instead of panicking.
//! On-disk bytes are untrusted: a torn write or bit flip must become an
//! error, never an out-of-bounds panic.

pub fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Returns `(value, bytes_used)`.
pub fn get_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut v: u64 = 0;
    for (i, &b) in buf.iter().enumerate().take(10) {
        v |= u64::from(b & 0x7f) << (7 * i);
        if b < 0x80 {
            return Some((v, i + 1));
        }
    }
    None
}

pub fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn get_u32(buf: &[u8], at: usize) -> Option<u32> {
    let b = buf.get(at..at.checked_add(4)?)?;
    Some(u32::from_le_bytes(b.try_into().ok()?))
}

pub fn get_u64(buf: &[u8], at: usize) -> Option<u64> {
    let b = buf.get(at..at.checked_add(8)?)?;
    Some(u64::from_le_bytes(b.try_into().ok()?))
}

/// Length-prefixed byte string.
pub fn put_slice(buf: &mut Vec<u8>, s: &[u8]) {
    put_varint(buf, s.len() as u64);
    buf.extend_from_slice(s);
}

/// Reads a length-prefixed byte string. Returns `(slice, bytes_used)`.
pub fn get_slice(buf: &[u8]) -> Option<(&[u8], usize)> {
    let (len, n) = get_varint(buf)?;
    let len = usize::try_from(len).ok()?;
    let end = n.checked_add(len)?;
    Some((buf.get(n..end)?, end))
}

/// CRC32C, masked like LevelDB does it.
///
/// Why mask? A CRC of data that itself contains CRCs (a WAL record holding
/// an SST block, say) has weak properties. Rotating + adding a constant
/// breaks that.
pub fn masked_crc(data: &[u8]) -> u32 {
    mask_crc(crc32c::crc32c(data))
}

pub fn mask_crc(c: u32) -> u32 {
    c.rotate_right(15).wrapping_add(0xa282_ead8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut b = Vec::new();
            put_varint(&mut b, v);
            assert_eq!(get_varint(&b), Some((v, b.len())));
        }
    }

    #[test]
    fn short_input_is_none_not_panic() {
        assert_eq!(get_varint(&[0x80, 0x80]), None);
        assert_eq!(get_u32(&[1, 2, 3], 0), None);
        assert_eq!(get_u64(&[0; 8], 1), None);
        assert_eq!(get_slice(&[5, 1, 2]), None);
        assert_eq!(get_u32(&[0; 8], usize::MAX), None);
    }
}
