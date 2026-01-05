//! Hash functions for CRUSH placement.
//!
//! CRUSH uses deterministic hash functions to map objects to placement groups
//! and to select items within buckets. The hash must be:
//! - Deterministic: same input always produces same output
//! - Uniform: output is uniformly distributed
//! - Fast: used for every placement decision

use std::hash::{Hash, Hasher};

use siphasher::sip::SipHasher13;

/// CRUSH hash key for consistent hashing across all nodes.
const CRUSH_HASH_KEY: (u64, u64) = (0x0706_0504_0302_0100, 0x0f0e_0d0c_0b0a_0908);

/// Compute a CRUSH hash for the given input.
///
/// Uses SipHash-1-3 for speed while maintaining good distribution.
/// The hash is seeded with a fixed key for cross-node consistency.
#[inline]
#[must_use]
pub fn crush_hash(input: u64) -> u64 {
    let mut hasher = SipHasher13::new_with_keys(CRUSH_HASH_KEY.0, CRUSH_HASH_KEY.1);
    input.hash(&mut hasher);
    hasher.finish()
}

/// Compute a CRUSH hash combining multiple inputs.
///
/// Used when we need to combine placement group ID with a round number
/// for replica selection.
#[inline]
#[must_use]
pub fn crush_hash2(a: u64, b: u64) -> u64 {
    let mut hasher = SipHasher13::new_with_keys(CRUSH_HASH_KEY.0, CRUSH_HASH_KEY.1);
    a.hash(&mut hasher);
    b.hash(&mut hasher);
    hasher.finish()
}

/// Compute a CRUSH hash combining three inputs.
#[inline]
#[must_use]
pub fn crush_hash3(a: u64, b: u64, c: u64) -> u64 {
    let mut hasher = SipHasher13::new_with_keys(CRUSH_HASH_KEY.0, CRUSH_HASH_KEY.1);
    a.hash(&mut hasher);
    b.hash(&mut hasher);
    c.hash(&mut hasher);
    hasher.finish()
}

/// Hash a string to a u64 for use as placement group seed.
#[inline]
#[must_use]
pub fn crush_hash_string(s: &str) -> u64 {
    let mut hasher = SipHasher13::new_with_keys(CRUSH_HASH_KEY.0, CRUSH_HASH_KEY.1);
    s.hash(&mut hasher);
    hasher.finish()
}

/// Compute placement group from bucket and key.
///
/// Returns a placement group ID in the range [0, pg_count).
#[inline]
#[must_use]
pub fn compute_pg(bucket: &str, key: &str, pg_count: u32) -> u32 {
    let hash = crush_hash_string(&format!("{bucket}/{key}"));
    (hash % u64::from(pg_count)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crush_hash_deterministic() {
        let h1 = crush_hash(42);
        let h2 = crush_hash(42);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_crush_hash_different_inputs() {
        let h1 = crush_hash(1);
        let h2 = crush_hash(2);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_crush_hash2_order_matters() {
        let h1 = crush_hash2(1, 2);
        let h2 = crush_hash2(2, 1);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_crush_hash_string() {
        let h1 = crush_hash_string("test-bucket/object-key");
        let h2 = crush_hash_string("test-bucket/object-key");
        assert_eq!(h1, h2);

        let h3 = crush_hash_string("other-bucket/object-key");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_compute_pg_range() {
        for pg_count in [1, 16, 256, 1024] {
            for i in 0..1000 {
                let pg = compute_pg("bucket", &format!("key{i}"), pg_count);
                assert!(pg < pg_count);
            }
        }
    }

    #[test]
    fn test_compute_pg_distribution() {
        let pg_count = 256;
        let mut counts = vec![0u32; pg_count as usize];

        for i in 0..10000 {
            let pg = compute_pg("test-bucket", &format!("object-{i}"), pg_count);
            counts[pg as usize] += 1;
        }

        // Check distribution is reasonably uniform (within 60% of expected)
        let expected = 10000.0 / 256.0;
        for count in counts {
            let ratio = f64::from(count) / expected;
            assert!(ratio > 0.4 && ratio < 1.6, "Distribution too skewed: {ratio}");
        }
    }
}
