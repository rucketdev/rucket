//! Hybrid Logical Clock (HLC) implementation for distributed causality tracking.
//!
//! HLC combines physical wall-clock time with a logical counter to provide
//! monotonically increasing timestamps that respect causality across distributed
//! nodes. This implementation follows the design from Kulkarni et al.
//!
//! # Format
//!
//! The timestamp is packed into a 64-bit value:
//! - Bits 16-63 (48 bits): Physical time in milliseconds since Unix epoch
//! - Bits 0-15 (16 bits): Logical counter (up to 65535 events per millisecond)
//!
//! This layout allows raw u64 comparison to correctly order timestamps
//! (physical time first, then logical counter). The 48-bit physical time
//! supports dates until year 10889.
//!
//! # Example
//!
//! ```
//! use rucket_core::hlc::{HlcClock, HlcTimestamp};
//!
//! // Create a clock for local operations
//! let clock = HlcClock::new();
//!
//! // Generate timestamps for local events
//! let ts1 = clock.now();
//! let ts2 = clock.now();
//! assert!(ts2 > ts1);
//!
//! // Update clock when receiving a remote timestamp
//! let remote_ts = HlcTimestamp::from_parts(ts1.physical_time(), 100);
//! let ts3 = clock.update(remote_ts);
//! assert!(ts3 > remote_ts);
//! assert!(ts3 > ts2);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Maximum clock skew allowed between nodes (500ms).
/// Timestamps from nodes with greater skew will be rejected.
pub const MAX_CLOCK_SKEW_MS: u64 = 500;

/// Mask for the logical counter component (lower 16 bits).
const LOGICAL_MASK: u64 = 0x0000_0000_0000_FFFF;

/// Number of bits for the logical counter component.
const LOGICAL_BITS: u32 = 16;

/// Maximum logical counter value (16 bits).
const MAX_LOGICAL: u16 = u16::MAX;

/// A hybrid logical clock timestamp.
///
/// Combines physical wall-clock time with a logical counter to ensure
/// monotonicity and causality tracking across distributed nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct HlcTimestamp(u64);

impl HlcTimestamp {
    /// Creates a new timestamp with the given physical time and logical counter.
    ///
    /// # Arguments
    ///
    /// * `physical_ms` - Physical time in milliseconds since Unix epoch (lower 48 bits used)
    /// * `logical` - Logical counter value
    #[must_use]
    pub const fn from_parts(physical_ms: u64, logical: u16) -> Self {
        // Physical time in high 48 bits, logical counter in low 16 bits
        let packed = (physical_ms << LOGICAL_BITS) | (logical as u64);
        Self(packed)
    }

    /// Creates a timestamp from a raw packed u64 value.
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw packed u64 value.
    #[must_use]
    pub const fn as_raw(&self) -> u64 {
        self.0
    }

    /// Returns the physical time component in milliseconds since Unix epoch.
    #[must_use]
    pub const fn physical_time(&self) -> u64 {
        self.0 >> LOGICAL_BITS
    }

    /// Returns the logical counter component.
    #[must_use]
    pub const fn logical(&self) -> u16 {
        (self.0 & LOGICAL_MASK) as u16
    }

    /// Creates a zero timestamp (epoch).
    #[must_use]
    pub const fn zero() -> Self {
        Self(0)
    }

    /// Returns true if this is a zero/unset timestamp.
    #[must_use]
    pub const fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl Default for HlcTimestamp {
    fn default() -> Self {
        Self::zero()
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by physical time first, then by logical counter
        // This is equivalent to comparing the raw packed values
        self.0.cmp(&other.0)
    }
}

impl std::fmt::Display for HlcTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.physical_time(), self.logical())
    }
}

impl From<u64> for HlcTimestamp {
    fn from(raw: u64) -> Self {
        Self::from_raw(raw)
    }
}

impl From<HlcTimestamp> for u64 {
    fn from(ts: HlcTimestamp) -> Self {
        ts.as_raw()
    }
}

/// A hybrid logical clock for generating monotonically increasing timestamps.
///
/// The clock is thread-safe and lock-free, using atomic operations to ensure
/// correct behavior under concurrent access.
#[derive(Debug)]
pub struct HlcClock {
    /// The last timestamp generated, packed as u64.
    last: AtomicU64,
}

impl HlcClock {
    /// Creates a new HLC clock.
    #[must_use]
    pub fn new() -> Self {
        Self { last: AtomicU64::new(0) }
    }

    /// Generates a new timestamp for a local event.
    ///
    /// The returned timestamp is guaranteed to be greater than any previously
    /// generated timestamp from this clock.
    pub fn now(&self) -> HlcTimestamp {
        let wall = wall_clock_ms();

        loop {
            let last = self.last.load(Ordering::Acquire);
            let last_ts = HlcTimestamp::from_raw(last);

            let new_ts = if wall > last_ts.physical_time() {
                // Wall clock advanced, reset logical counter
                HlcTimestamp::from_parts(wall, 0)
            } else {
                // Wall clock hasn't advanced, increment logical counter
                let new_logical = last_ts.logical().saturating_add(1);
                if new_logical == MAX_LOGICAL {
                    // Logical counter overflow - use wall clock + 1ms
                    // This should be extremely rare in practice
                    HlcTimestamp::from_parts(last_ts.physical_time() + 1, 0)
                } else {
                    HlcTimestamp::from_parts(last_ts.physical_time(), new_logical)
                }
            };

            // Try to update atomically
            if self
                .last
                .compare_exchange(last, new_ts.as_raw(), Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return new_ts;
            }
            // CAS failed, retry with updated value
        }
    }

    /// Updates the clock with a remote timestamp and returns a new timestamp.
    ///
    /// This should be called when receiving a message from another node to
    /// ensure causality is preserved.
    ///
    /// # Arguments
    ///
    /// * `remote` - The timestamp received from a remote node
    ///
    /// # Returns
    ///
    /// A new timestamp that is greater than both the local clock and the
    /// remote timestamp.
    pub fn update(&self, remote: HlcTimestamp) -> HlcTimestamp {
        let wall = wall_clock_ms();

        loop {
            let last = self.last.load(Ordering::Acquire);
            let last_ts = HlcTimestamp::from_raw(last);

            let max_physical = wall.max(last_ts.physical_time()).max(remote.physical_time());

            let new_ts = if max_physical == wall
                && wall > last_ts.physical_time()
                && wall > remote.physical_time()
            {
                // Wall clock is ahead of both, reset logical counter
                HlcTimestamp::from_parts(wall, 0)
            } else if max_physical == last_ts.physical_time()
                && last_ts.physical_time() == remote.physical_time()
            {
                // All three are equal, take max logical + 1
                let max_logical = last_ts.logical().max(remote.logical());
                let new_logical = max_logical.saturating_add(1);
                if new_logical == MAX_LOGICAL {
                    HlcTimestamp::from_parts(max_physical + 1, 0)
                } else {
                    HlcTimestamp::from_parts(max_physical, new_logical)
                }
            } else if max_physical == last_ts.physical_time() {
                // Local is max
                let new_logical = last_ts.logical().saturating_add(1);
                if new_logical == MAX_LOGICAL {
                    HlcTimestamp::from_parts(max_physical + 1, 0)
                } else {
                    HlcTimestamp::from_parts(max_physical, new_logical)
                }
            } else if max_physical == remote.physical_time() {
                // Remote is max
                let new_logical = remote.logical().saturating_add(1);
                if new_logical == MAX_LOGICAL {
                    HlcTimestamp::from_parts(max_physical + 1, 0)
                } else {
                    HlcTimestamp::from_parts(max_physical, new_logical)
                }
            } else {
                // Wall clock is max but equal to one of the others
                HlcTimestamp::from_parts(wall, 0)
            };

            // Try to update atomically
            if self
                .last
                .compare_exchange(last, new_ts.as_raw(), Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return new_ts;
            }
            // CAS failed, retry with updated value
        }
    }

    /// Checks if a remote timestamp has acceptable clock skew.
    ///
    /// Returns `true` if the remote timestamp is within the acceptable skew
    /// threshold, `false` otherwise.
    #[must_use]
    pub fn is_acceptable_skew(&self, remote: HlcTimestamp) -> bool {
        let wall = wall_clock_ms();
        let remote_physical = remote.physical_time();

        // Remote timestamp should not be more than MAX_CLOCK_SKEW_MS in the future
        remote_physical <= wall + MAX_CLOCK_SKEW_MS
    }

    /// Returns the current timestamp without advancing the clock.
    #[must_use]
    pub fn current(&self) -> HlcTimestamp {
        HlcTimestamp::from_raw(self.last.load(Ordering::Acquire))
    }
}

impl Default for HlcClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns the current wall clock time in milliseconds since Unix epoch.
fn wall_clock_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("system time before Unix epoch").as_millis()
        as u64
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn test_timestamp_from_parts() {
        let ts = HlcTimestamp::from_parts(1000, 42);
        assert_eq!(ts.physical_time(), 1000);
        assert_eq!(ts.logical(), 42);
    }

    #[test]
    fn test_timestamp_max_physical() {
        // 48 bits of physical time
        let max_physical = (1u64 << 48) - 1;
        let ts = HlcTimestamp::from_parts(max_physical, 0);
        assert_eq!(ts.physical_time(), max_physical);
        assert_eq!(ts.logical(), 0);
    }

    #[test]
    fn test_timestamp_max_logical() {
        let ts = HlcTimestamp::from_parts(1000, u16::MAX);
        assert_eq!(ts.physical_time(), 1000);
        assert_eq!(ts.logical(), u16::MAX);
    }

    #[test]
    fn test_timestamp_ordering() {
        let ts1 = HlcTimestamp::from_parts(1000, 0);
        let ts2 = HlcTimestamp::from_parts(1000, 1);
        let ts3 = HlcTimestamp::from_parts(1001, 0);

        assert!(ts1 < ts2);
        assert!(ts2 < ts3);
        assert!(ts1 < ts3);
    }

    #[test]
    fn test_timestamp_equality() {
        let ts1 = HlcTimestamp::from_parts(1000, 42);
        let ts2 = HlcTimestamp::from_parts(1000, 42);
        assert_eq!(ts1, ts2);
    }

    #[test]
    fn test_timestamp_zero() {
        let ts = HlcTimestamp::zero();
        assert!(ts.is_zero());
        assert_eq!(ts.physical_time(), 0);
        assert_eq!(ts.logical(), 0);
    }

    #[test]
    fn test_timestamp_display() {
        let ts = HlcTimestamp::from_parts(1234567890, 42);
        assert_eq!(format!("{ts}"), "1234567890:42");
    }

    #[test]
    fn test_timestamp_serde() {
        let ts = HlcTimestamp::from_parts(1234567890, 42);
        let json = serde_json::to_string(&ts).unwrap();
        let ts2: HlcTimestamp = serde_json::from_str(&json).unwrap();
        assert_eq!(ts, ts2);
    }

    #[test]
    fn test_clock_monotonic() {
        let clock = HlcClock::new();
        let mut prev = clock.now();

        for _ in 0..1000 {
            let curr = clock.now();
            assert!(curr > prev, "timestamps must be monotonically increasing");
            prev = curr;
        }
    }

    #[test]
    fn test_clock_update_advances() {
        let clock = HlcClock::new();

        // Generate local timestamp
        let ts1 = clock.now();

        // Simulate receiving a remote timestamp in the future
        let remote = HlcTimestamp::from_parts(ts1.physical_time() + 1000, 50);
        let ts2 = clock.update(remote);

        assert!(ts2 > ts1);
        assert!(ts2 > remote);
    }

    #[test]
    fn test_clock_update_same_physical() {
        let clock = HlcClock::new();

        let ts1 = clock.now();
        let remote = HlcTimestamp::from_parts(ts1.physical_time(), ts1.logical() + 10);
        let ts2 = clock.update(remote);

        assert!(ts2 > ts1);
        assert!(ts2 > remote);
    }

    #[test]
    fn test_clock_skew_detection() {
        let clock = HlcClock::new();
        let wall = wall_clock_ms();

        // Timestamp slightly in the future (acceptable)
        let acceptable = HlcTimestamp::from_parts(wall + 100, 0);
        assert!(clock.is_acceptable_skew(acceptable));

        // Timestamp far in the future (not acceptable)
        let too_far = HlcTimestamp::from_parts(wall + MAX_CLOCK_SKEW_MS + 1000, 0);
        assert!(!clock.is_acceptable_skew(too_far));
    }

    #[test]
    fn test_clock_concurrent() {
        use std::sync::Arc;

        let clock = Arc::new(HlcClock::new());
        let mut handles = vec![];

        // Spawn multiple threads generating timestamps concurrently
        for _ in 0..4 {
            let clock = Arc::clone(&clock);
            handles.push(thread::spawn(move || {
                let mut timestamps = Vec::with_capacity(1000);
                for _ in 0..1000 {
                    timestamps.push(clock.now());
                }
                timestamps
            }));
        }

        // Collect all timestamps
        let mut all_timestamps: Vec<HlcTimestamp> = vec![];
        for handle in handles {
            all_timestamps.extend(handle.join().unwrap());
        }

        // Verify all timestamps are unique
        all_timestamps.sort();
        for i in 1..all_timestamps.len() {
            assert_ne!(all_timestamps[i - 1], all_timestamps[i], "all timestamps must be unique");
        }
    }

    #[test]
    fn test_raw_roundtrip() {
        let ts = HlcTimestamp::from_parts(9876543210, 1234);
        let raw = ts.as_raw();
        let ts2 = HlcTimestamp::from_raw(raw);
        assert_eq!(ts, ts2);
    }

    #[test]
    fn test_u64_conversion() {
        let ts = HlcTimestamp::from_parts(1000, 42);
        let raw: u64 = ts.into();
        let ts2: HlcTimestamp = raw.into();
        assert_eq!(ts, ts2);
    }
}
