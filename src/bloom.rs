//! Bloom filter over the user keys of one SSTable.
//!
//! A "no" is always right, so a miss skips the index lookup and the data
//! block read entirely. A "maybe" is wrong about 1% of the time at 10 bits
//! per key.
//!
//! Hashing: one 64-bit xxh3 hash per key, split into two 32-bit halves, and
//! probe `i` is `h1 + i * h2` (Kirsch–Mitzenmacher double hashing). Same false
//! positive rate as k independent hashes, at the cost of one hash.
//!
//! The hash must be stable across Rust versions because it's stored on disk.
//! That rules out `std`'s `DefaultHasher`.

use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit array followed by one byte holding `k`.
    data: bytes::Bytes,
}

#[inline]
fn hash(key: &[u8]) -> u64 {
    xxh3_64(key)
}

/// Collects key hashes while an SSTable is built, then emits the filter.
#[derive(Debug, Default)]
pub struct BloomBuilder {
    hashes: Vec<u64>,
}

impl BloomBuilder {
    pub fn add(&mut self, user_key: &[u8]) {
        self.hashes.push(hash(user_key));
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    pub fn finish(&self, bits_per_key: usize) -> Vec<u8> {
        // k = bits_per_key * ln(2) is the optimum. Clamp to sane bounds.
        let k = ((bits_per_key as f64) * std::f64::consts::LN_2).round() as usize;
        let k = k.clamp(1, 30);
        // Tiny filters have bad false-positive rates, so use at least 64 bits.
        let nbits = (self.hashes.len() * bits_per_key).max(64);
        let nbytes = nbits.div_ceil(8);
        let nbits = nbytes * 8;
        let mut out = vec![0u8; nbytes + 1];
        for &h in &self.hashes {
            let h1 = h as u32;
            let h2 = (h >> 32) as u32;
            for i in 0..k as u32 {
                let bit = (h1.wrapping_add(i.wrapping_mul(h2)) as usize) % nbits;
                out[bit / 8] |= 1 << (bit % 8);
            }
        }
        out[nbytes] = k as u8;
        out
    }
}

impl BloomFilter {
    /// An empty `data` means "no filter": every key may be present.
    pub fn new(data: bytes::Bytes) -> Self {
        BloomFilter { data }
    }

    pub fn may_contain(&self, user_key: &[u8]) -> bool {
        let Some((&k, bits)) = self.data.split_last() else {
            return true;
        };
        if bits.is_empty() || k == 0 || k > 30 {
            // Unknown encoding: never claim "absent". A wrong "no" is data loss.
            return true;
        }
        let nbits = bits.len() * 8;
        let h = hash(user_key);
        let h1 = h as u32;
        let h2 = (h >> 32) as u32;
        for i in 0..u32::from(k) {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2)) as usize) % nbits;
            if bits[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(n: u32, bits: usize) -> BloomFilter {
        let mut b = BloomBuilder::default();
        for i in 0..n {
            b.add(format!("key{i}").as_bytes());
        }
        BloomFilter::new(b.finish(bits).into())
    }

    #[test]
    fn no_false_negatives() {
        let f = build(10_000, 10);
        for i in 0..10_000 {
            assert!(f.may_contain(format!("key{i}").as_bytes()));
        }
    }

    #[test]
    fn false_positive_rate_near_one_percent() {
        let f = build(100_000, 10);
        let probes = 100_000;
        let fp = (0..probes).filter(|i| f.may_contain(format!("missing{i}").as_bytes())).count();
        let rate = fp as f64 / probes as f64;
        // Theory for k=7, 10 bits/key: ~0.82%.
        assert!(rate < 0.015, "false positive rate too high: {rate}");
    }

    #[test]
    fn empty_filter_says_maybe() {
        let f = BloomFilter::new(bytes::Bytes::new());
        assert!(f.may_contain(b"anything"));
    }
}
