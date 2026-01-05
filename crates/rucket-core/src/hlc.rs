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
//! # Production Features
//!
//! This implementation includes production-ready features:
//! - **Clock skew detection**: Rejects timestamps > 500ms in the future
//! - **Drift tracking**: Monitors accumulated clock drift for health checks
//! - **Thread safety**: Lock-free atomic operations for concurrent access
//! - **Error handling**: Proper error types for clock skew violations
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
//!
//! # Clock Skew Handling
//!
//! ```
//! use rucket_core::hlc::{HlcClock, HlcTimestamp, ClockError};
//!
//! let clock = HlcClock::new();
//!
//! // Timestamps with acceptable skew pass validation
//! let ts = clock.now();
//! assert!(clock.validate_timestamp(ts).is_ok());
//!
//! // Timestamps too far in the future are rejected
//! let future_ts = HlcTimestamp::from_parts(ts.physical_time() + 600_000, 0);
//! assert!(clock.validate_timestamp(future_ts).is_err());
//! ```

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Maximum clock skew allowed between nodes (500ms).
/// Timestamps from nodes with greater skew will be rejected.
pub const MAX_CLOCK_SKEW_MS: u64 = 500;

/// Threshold for warning about clock drift (100ms).
/// If accumulated drift exceeds this, the clock may need NTP sync.
pub const DRIFT_WARNING_THRESHOLD_MS: i64 = 100;

/// Error type for HLC operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ClockError {
    /// Remote timestamp is too far in the future (clock skew violation).
    #[error("clock skew violation: remote timestamp {remote_ms}ms is {skew_ms}ms ahead of local time (max allowed: {max_skew_ms}ms)")]
    ClockSkewViolation {
        /// Remote timestamp physical time in milliseconds.
        remote_ms: u64,
        /// Observed skew in milliseconds.
        skew_ms: u64,
        /// Maximum allowed skew in milliseconds.
        max_skew_ms: u64,
    },

    /// Logical counter overflow (extremely rare, >65535 events/ms).
    #[error("logical counter overflow: more than 65535 events in a single millisecond")]
    LogicalCounterOverflow,
}

/// Result type for HLC operations.
pub type ClockResult<T> = std::result::Result<T, ClockError>;

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
///
/// # Drift Tracking
///
/// The clock tracks accumulated drift between the HLC physical time and the
/// actual wall clock. This can be used to detect nodes with clock issues
/// that may need NTP synchronization.
#[derive(Debug)]
pub struct HlcClock {
    /// The last timestamp generated, packed as u64.
    last: AtomicU64,
    /// Accumulated drift from wall clock in milliseconds (can be negative).
    /// Positive means HLC is ahead of wall clock, negative means behind.
    drift_ms: AtomicI64,
    /// Maximum observed forward skew in milliseconds.
    max_forward_skew_ms: AtomicU64,
}

impl HlcClock {
    /// Creates a new HLC clock.
    #[must_use]
    pub fn new() -> Self {
        Self {
            last: AtomicU64::new(0),
            drift_ms: AtomicI64::new(0),
            max_forward_skew_ms: AtomicU64::new(0),
        }
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
                // Record drift for monitoring
                self.record_drift(new_ts.physical_time(), wall);
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

    /// Validates a remote timestamp and returns an error if clock skew exceeds threshold.
    ///
    /// This is the production-ready version of `is_acceptable_skew` that provides
    /// detailed error information for debugging and logging.
    ///
    /// # Errors
    ///
    /// Returns `ClockError::ClockSkewViolation` if the remote timestamp is more than
    /// `MAX_CLOCK_SKEW_MS` (500ms) ahead of the local wall clock.
    pub fn validate_timestamp(&self, remote: HlcTimestamp) -> ClockResult<()> {
        let wall = wall_clock_ms();
        let remote_physical = remote.physical_time();

        if remote_physical > wall + MAX_CLOCK_SKEW_MS {
            let skew = remote_physical - wall;
            // Track max observed skew for monitoring
            self.max_forward_skew_ms.fetch_max(skew, Ordering::Relaxed);
            return Err(ClockError::ClockSkewViolation {
                remote_ms: remote_physical,
                skew_ms: skew,
                max_skew_ms: MAX_CLOCK_SKEW_MS,
            });
        }

        Ok(())
    }

    /// Updates the clock with a remote timestamp, validating clock skew first.
    ///
    /// This is the production-ready version of `update` that rejects timestamps
    /// with excessive clock skew before updating the local clock.
    ///
    /// # Errors
    ///
    /// Returns `ClockError::ClockSkewViolation` if the remote timestamp is more than
    /// `MAX_CLOCK_SKEW_MS` (500ms) ahead of the local wall clock.
    pub fn update_checked(&self, remote: HlcTimestamp) -> ClockResult<HlcTimestamp> {
        self.validate_timestamp(remote)?;
        Ok(self.update(remote))
    }

    /// Returns the current timestamp without advancing the clock.
    #[must_use]
    pub fn current(&self) -> HlcTimestamp {
        HlcTimestamp::from_raw(self.last.load(Ordering::Acquire))
    }

    /// Returns the accumulated drift from wall clock in milliseconds.
    ///
    /// Positive values indicate the HLC is ahead of wall clock,
    /// negative values indicate it's behind.
    #[must_use]
    pub fn drift_ms(&self) -> i64 {
        self.drift_ms.load(Ordering::Relaxed)
    }

    /// Returns the maximum forward skew observed in milliseconds.
    ///
    /// This is useful for monitoring and alerting on clock issues.
    #[must_use]
    pub fn max_forward_skew_ms(&self) -> u64 {
        self.max_forward_skew_ms.load(Ordering::Relaxed)
    }

    /// Returns true if the clock has excessive drift that may need attention.
    ///
    /// Excessive drift (> 100ms) may indicate NTP synchronization issues.
    #[must_use]
    pub fn has_excessive_drift(&self) -> bool {
        self.drift_ms().abs() > DRIFT_WARNING_THRESHOLD_MS
    }

    /// Returns clock health metrics for monitoring.
    #[must_use]
    pub fn health_metrics(&self) -> ClockHealthMetrics {
        let wall = wall_clock_ms();
        let current = self.current();
        let current_drift =
            if current.is_zero() { 0 } else { current.physical_time() as i64 - wall as i64 };

        ClockHealthMetrics {
            current_timestamp: current,
            wall_clock_ms: wall,
            current_drift_ms: current_drift,
            accumulated_drift_ms: self.drift_ms(),
            max_forward_skew_ms: self.max_forward_skew_ms(),
            is_healthy: current_drift.abs() <= MAX_CLOCK_SKEW_MS as i64,
        }
    }

    /// Records drift for tracking purposes (called internally after timestamp generation).
    fn record_drift(&self, hlc_physical: u64, wall: u64) {
        let drift = hlc_physical as i64 - wall as i64;
        // Use relaxed ordering since this is just for monitoring
        self.drift_ms.store(drift, Ordering::Relaxed);
    }
}

/// Health metrics for an HLC clock.
///
/// Used for monitoring and alerting on clock issues in production.
#[derive(Debug, Clone, Copy)]
pub struct ClockHealthMetrics {
    /// Current HLC timestamp.
    pub current_timestamp: HlcTimestamp,
    /// Current wall clock time in milliseconds.
    pub wall_clock_ms: u64,
    /// Current drift from wall clock in milliseconds.
    pub current_drift_ms: i64,
    /// Accumulated drift over time.
    pub accumulated_drift_ms: i64,
    /// Maximum forward skew observed.
    pub max_forward_skew_ms: u64,
    /// Whether the clock is within healthy parameters.
    pub is_healthy: bool,
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

    // Production-ready feature tests

    #[test]
    fn test_validate_timestamp_acceptable() {
        let clock = HlcClock::new();
        let wall = wall_clock_ms();

        // Timestamp at current time is acceptable
        let ts = HlcTimestamp::from_parts(wall, 0);
        assert!(clock.validate_timestamp(ts).is_ok());

        // Timestamp slightly in the future (within threshold) is acceptable
        let ts = HlcTimestamp::from_parts(wall + 100, 0);
        assert!(clock.validate_timestamp(ts).is_ok());

        // Timestamp at the threshold is acceptable
        let ts = HlcTimestamp::from_parts(wall + MAX_CLOCK_SKEW_MS, 0);
        assert!(clock.validate_timestamp(ts).is_ok());
    }

    #[test]
    fn test_validate_timestamp_rejected() {
        let clock = HlcClock::new();
        let wall = wall_clock_ms();

        // Timestamp just beyond threshold is rejected
        let future_ts = HlcTimestamp::from_parts(wall + MAX_CLOCK_SKEW_MS + 1, 0);
        let result = clock.validate_timestamp(future_ts);
        assert!(result.is_err());

        if let Err(ClockError::ClockSkewViolation { remote_ms, skew_ms, max_skew_ms }) = result {
            assert_eq!(remote_ms, wall + MAX_CLOCK_SKEW_MS + 1);
            assert!(skew_ms > MAX_CLOCK_SKEW_MS);
            assert_eq!(max_skew_ms, MAX_CLOCK_SKEW_MS);
        } else {
            panic!("Expected ClockSkewViolation error");
        }
    }

    #[test]
    fn test_validate_timestamp_far_future() {
        let clock = HlcClock::new();
        let wall = wall_clock_ms();

        // Timestamp very far in the future
        let far_future = HlcTimestamp::from_parts(wall + 60_000, 0); // 1 minute ahead
        let result = clock.validate_timestamp(far_future);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_checked_success() {
        let clock = HlcClock::new();
        let wall = wall_clock_ms();

        // Remote timestamp within threshold
        let remote = HlcTimestamp::from_parts(wall + 100, 0);
        let result = clock.update_checked(remote);
        assert!(result.is_ok());

        let new_ts = result.unwrap();
        assert!(new_ts > remote);
    }

    #[test]
    fn test_update_checked_rejected() {
        let clock = HlcClock::new();
        let wall = wall_clock_ms();

        // Remote timestamp beyond threshold
        let remote = HlcTimestamp::from_parts(wall + MAX_CLOCK_SKEW_MS + 1000, 0);
        let result = clock.update_checked(remote);
        assert!(result.is_err());

        // Clock should not be updated on rejection
        let current = clock.current();
        assert!(current.physical_time() < wall + MAX_CLOCK_SKEW_MS);
    }

    #[test]
    fn test_clock_health_metrics() {
        let clock = HlcClock::new();

        // Generate some timestamps
        for _ in 0..10 {
            clock.now();
        }

        let metrics = clock.health_metrics();
        assert!(metrics.is_healthy);
        assert!(metrics.wall_clock_ms > 0);
        assert!(!metrics.current_timestamp.is_zero());
    }

    #[test]
    fn test_max_forward_skew_tracking() {
        let clock = HlcClock::new();
        let wall = wall_clock_ms();

        // Initially zero
        assert_eq!(clock.max_forward_skew_ms(), 0);

        // Validate a timestamp far in the future - should be rejected and skew recorded
        let future = HlcTimestamp::from_parts(wall + 10_000, 0);
        let _ = clock.validate_timestamp(future);

        // Max skew should be recorded
        assert!(clock.max_forward_skew_ms() > MAX_CLOCK_SKEW_MS);
    }

    #[test]
    fn test_drift_tracking() {
        let clock = HlcClock::new();

        // Initially zero drift
        assert_eq!(clock.drift_ms(), 0);

        // Generate timestamp - drift should be minimal for fresh clock
        clock.now();
        let drift = clock.drift_ms();
        // Drift should be very small for a new clock
        assert!(drift.abs() < 100);
    }

    #[test]
    fn test_causality_across_clocks() {
        let clock_a = HlcClock::new();
        let clock_b = HlcClock::new();

        // A generates event
        let ts_a1 = clock_a.now();

        // B receives A's event and generates response
        let ts_b1 = clock_b.update(ts_a1);
        assert!(ts_b1 > ts_a1);

        // A receives B's response
        let ts_a2 = clock_a.update(ts_b1);
        assert!(ts_a2 > ts_b1);
        assert!(ts_a2 > ts_a1);

        // Causality chain is preserved
        assert!(ts_a1 < ts_b1);
        assert!(ts_b1 < ts_a2);
    }

    #[test]
    fn test_causality_with_multiple_events() {
        let clock_a = HlcClock::new();
        let clock_b = HlcClock::new();
        let clock_c = HlcClock::new();

        // Multiple concurrent events
        let events_a: Vec<_> = (0..10).map(|_| clock_a.now()).collect();
        let events_b: Vec<_> = (0..10).map(|_| clock_b.now()).collect();

        // C receives all events
        let mut all_events = events_a.clone();
        all_events.extend(&events_b);

        for event in &all_events {
            clock_c.update(*event);
        }

        // C generates new event - must be after all received
        let ts_c = clock_c.now();
        for event in &all_events {
            assert!(ts_c > *event, "C's timestamp must be after all received events");
        }
    }

    #[test]
    fn test_clock_error_display() {
        let err =
            ClockError::ClockSkewViolation { remote_ms: 1000, skew_ms: 600, max_skew_ms: 500 };
        let msg = format!("{err}");
        assert!(msg.contains("clock skew violation"));
        assert!(msg.contains("600"));
        assert!(msg.contains("500"));

        let err = ClockError::LogicalCounterOverflow;
        let msg = format!("{err}");
        assert!(msg.contains("logical counter overflow"));
    }

    #[test]
    fn test_has_excessive_drift() {
        let clock = HlcClock::new();

        // Fresh clock should not have excessive drift
        clock.now();
        assert!(!clock.has_excessive_drift());
    }

    #[test]
    fn test_past_timestamps_accepted() {
        let clock = HlcClock::new();
        let wall = wall_clock_ms();

        // Timestamps in the past are always acceptable (no forward skew)
        let past = HlcTimestamp::from_parts(wall - 10_000, 0);
        assert!(clock.validate_timestamp(past).is_ok());

        // Very old timestamps are still acceptable
        let very_old = HlcTimestamp::from_parts(1000, 0);
        assert!(clock.validate_timestamp(very_old).is_ok());
    }

    #[test]
    fn test_high_frequency_monotonicity() {
        let clock = HlcClock::new();
        let mut prev = clock.now();

        // Generate 100k timestamps rapidly
        for i in 0..100_000 {
            let curr = clock.now();
            assert!(
                curr > prev,
                "Monotonicity violated at iteration {}: {:?} <= {:?}",
                i,
                curr,
                prev
            );
            prev = curr;
        }
    }
}
