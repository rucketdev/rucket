//! Phi Accrual Failure Detector implementation.
//!
//! The Phi Accrual Failure Detector is a probabilistic failure detector that
//! computes a "suspicion level" (phi) based on heartbeat arrival times.
//! Unlike binary failure detectors, it provides a continuous suspicion value
//! that can be thresholded according to application needs.
//!
//! # Algorithm
//!
//! The detector maintains a window of recent heartbeat inter-arrival times
//! and models them as samples from a normal distribution. When a new heartbeat
//! should have arrived but hasn't, the detector computes phi as:
//!
//! phi = -log10(1 - CDF(now - last_heartbeat))
//!
//! where CDF is the cumulative distribution function of the normal distribution
//! fitted to the historical inter-arrival times.
//!
//! # Reference
//!
//! Hayashibara, N., Defago, X., Yared, R., & Katayama, T. (2004).
//! "The Phi Accrual Failure Detector"
//!
//! # Example
//!
//! ```
//! use rucket_cluster::phi_detector::{PhiAccrualDetector, PhiDetectorConfig};
//!
//! let config = PhiDetectorConfig::default();
//! let detector = PhiAccrualDetector::new(config);
//!
//! // Record heartbeat arrivals
//! detector.heartbeat("node-1");
//!
//! // Check if node is suspected to be failed
//! let phi = detector.phi("node-1").unwrap_or(0.0);
//! if phi > 8.0 {
//!     println!("Node suspected to be failed");
//! }
//! ```

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::{debug, trace};

/// Configuration for the Phi Accrual Failure Detector.
#[derive(Debug, Clone)]
pub struct PhiDetectorConfig {
    /// Phi threshold above which a node is considered failed.
    /// Higher values mean more tolerance for late heartbeats.
    /// Typical values: 8-12 for LAN, 16+ for WAN.
    pub threshold: f64,

    /// Expected heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,

    /// Maximum number of samples to keep in the sliding window.
    pub max_sample_size: usize,

    /// Minimum number of samples required before computing phi.
    /// Until this many samples are collected, nodes are assumed healthy.
    pub min_samples: usize,

    /// Initial estimate of heartbeat interval variance (std dev).
    /// Used when there are fewer than min_samples.
    pub initial_std_dev_ms: f64,

    /// Minimum standard deviation to prevent division issues.
    pub min_std_dev_ms: f64,

    /// Acceptable delay factor before starting to suspect.
    /// A value of 1.0 means suspect immediately after expected interval.
    pub acceptable_delay_factor: f64,
}

impl Default for PhiDetectorConfig {
    fn default() -> Self {
        Self {
            threshold: 8.0,               // ~99.997% confidence
            heartbeat_interval_ms: 1000,  // 1 second
            max_sample_size: 1000,        // Keep last 1000 samples
            min_samples: 10,              // Need 10 samples for statistics
            initial_std_dev_ms: 500.0,    // Initial variance estimate
            min_std_dev_ms: 10.0,         // Minimum std dev
            acceptable_delay_factor: 1.5, // Allow 50% slack
        }
    }
}

/// Per-node heartbeat tracking state.
struct NodeState {
    /// Sliding window of inter-arrival times in milliseconds.
    intervals: VecDeque<f64>,
    /// Time of the last heartbeat.
    last_heartbeat: Instant,
    /// Running mean of intervals.
    mean: f64,
    /// Running variance of intervals (using Welford's algorithm).
    variance: f64,
    /// Count of samples for running statistics.
    count: u64,
}

impl NodeState {
    fn new() -> Self {
        Self {
            intervals: VecDeque::new(),
            last_heartbeat: Instant::now(),
            mean: 0.0,
            variance: 0.0,
            count: 0,
        }
    }

    /// Records a new heartbeat and updates statistics.
    fn record_heartbeat(&mut self, config: &PhiDetectorConfig) {
        let now = Instant::now();
        let interval = now.duration_since(self.last_heartbeat).as_secs_f64() * 1000.0;
        self.last_heartbeat = now;

        // Add to sliding window
        self.intervals.push_back(interval);
        if self.intervals.len() > config.max_sample_size {
            self.intervals.pop_front();
        }

        // Update running statistics using Welford's algorithm
        self.count += 1;
        let delta = interval - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = interval - self.mean;
        self.variance += delta * delta2;
    }

    /// Returns the standard deviation of inter-arrival times.
    fn std_dev(&self, config: &PhiDetectorConfig) -> f64 {
        if self.count < 2 {
            return config.initial_std_dev_ms;
        }
        let variance = self.variance / (self.count - 1) as f64;
        variance.sqrt().max(config.min_std_dev_ms)
    }

    /// Returns the mean inter-arrival time.
    fn mean_interval(&self, config: &PhiDetectorConfig) -> f64 {
        if self.count == 0 {
            config.heartbeat_interval_ms as f64
        } else {
            self.mean
        }
    }

    /// Computes the phi value based on time since last heartbeat.
    fn compute_phi(&self, config: &PhiDetectorConfig) -> f64 {
        let now = Instant::now();
        let time_since_last = now.duration_since(self.last_heartbeat).as_secs_f64() * 1000.0;

        // If we don't have enough samples, use a simple threshold
        if self.count < config.min_samples as u64 {
            let expected = config.heartbeat_interval_ms as f64 * config.acceptable_delay_factor;
            if time_since_last < expected {
                return 0.0;
            }
            // Linear growth for simple mode
            return (time_since_last - expected) / config.heartbeat_interval_ms as f64;
        }

        let mean = self.mean_interval(config);
        let std_dev = self.std_dev(config);

        // Compute how many standard deviations away we are
        let z = (time_since_last - mean) / std_dev;

        // If heartbeat arrived early or on time, phi is 0
        if z <= 0.0 {
            return 0.0;
        }

        // Compute phi = -log10(1 - CDF(z))
        // Using the approximation for the normal CDF tail
        // For large z, 1 - CDF(z) ≈ exp(-z²/2) / (z * sqrt(2π))
        // So phi ≈ z²/(2*ln(10)) + log10(z*sqrt(2π))

        // More accurate approximation using error function
        let p = Self::normal_cdf(z);
        if p >= 1.0 {
            // Prevent log(0)
            return f64::MAX;
        }

        -f64::log10(1.0 - p)
    }

    /// Approximation of the normal CDF using the error function.
    fn normal_cdf(x: f64) -> f64 {
        // Using approximation: Φ(x) ≈ 0.5 * (1 + erf(x / sqrt(2)))
        0.5 * (1.0 + Self::erf(x / std::f64::consts::SQRT_2))
    }

    /// Approximation of the error function.
    /// Uses Horner's method for the polynomial approximation.
    fn erf(x: f64) -> f64 {
        // Abramowitz and Stegun approximation (7.1.26)
        // Maximum error: 1.5e-7
        let a1 = 0.254829592;
        let a2 = -0.284496736;
        let a3 = 1.421413741;
        let a4 = -1.453152027;
        let a5 = 1.061405429;
        let p = 0.3275911;

        let sign = if x < 0.0 { -1.0 } else { 1.0 };
        let x = x.abs();

        let t = 1.0 / (1.0 + p * x);
        let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

        sign * y
    }
}

/// Phi Accrual Failure Detector.
///
/// Thread-safe implementation that tracks multiple nodes.
pub struct PhiAccrualDetector {
    config: PhiDetectorConfig,
    nodes: DashMap<String, Arc<RwLock<NodeState>>>,
}

impl PhiAccrualDetector {
    /// Creates a new detector with the given configuration.
    pub fn new(config: PhiDetectorConfig) -> Self {
        Self { config, nodes: DashMap::new() }
    }

    /// Creates a new detector with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(PhiDetectorConfig::default())
    }

    /// Records a heartbeat from the given node.
    pub fn heartbeat(&self, node_id: &str) {
        let state = self.nodes.entry(node_id.to_string()).or_insert_with(|| {
            debug!(node_id = %node_id, "Registering new node in failure detector");
            Arc::new(RwLock::new(NodeState::new()))
        });

        let mut state = state.write();
        state.record_heartbeat(&self.config);

        trace!(
            node_id = %node_id,
            mean_ms = state.mean_interval(&self.config),
            std_dev_ms = state.std_dev(&self.config),
            sample_count = state.count,
            "Recorded heartbeat"
        );
    }

    /// Computes the phi value for the given node.
    ///
    /// Returns `None` if the node is unknown.
    pub fn phi(&self, node_id: &str) -> Option<f64> {
        self.nodes.get(node_id).map(|state| {
            let state = state.read();
            state.compute_phi(&self.config)
        })
    }

    /// Returns true if the node is suspected to be failed.
    ///
    /// A node is suspected if its phi value exceeds the configured threshold.
    pub fn is_suspected(&self, node_id: &str) -> bool {
        self.phi(node_id).is_none_or(|phi| phi > self.config.threshold)
    }

    /// Returns true if the node is considered healthy.
    pub fn is_healthy(&self, node_id: &str) -> bool {
        !self.is_suspected(node_id)
    }

    /// Returns a list of all suspected nodes.
    pub fn suspected_nodes(&self) -> Vec<String> {
        self.nodes
            .iter()
            .filter_map(|entry| {
                let phi = entry.value().read().compute_phi(&self.config);
                if phi > self.config.threshold {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns a list of all healthy nodes.
    pub fn healthy_nodes(&self) -> Vec<String> {
        self.nodes
            .iter()
            .filter_map(|entry| {
                let phi = entry.value().read().compute_phi(&self.config);
                if phi <= self.config.threshold {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Removes a node from tracking.
    pub fn remove_node(&self, node_id: &str) {
        self.nodes.remove(node_id);
        debug!(node_id = %node_id, "Removed node from failure detector");
    }

    /// Returns the time since the last heartbeat for a node.
    pub fn time_since_heartbeat(&self, node_id: &str) -> Option<Duration> {
        self.nodes.get(node_id).map(|state| {
            let state = state.read();
            Instant::now().duration_since(state.last_heartbeat)
        })
    }

    /// Returns the configuration.
    pub fn config(&self) -> &PhiDetectorConfig {
        &self.config
    }

    /// Returns the number of tracked nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns statistics for a node.
    pub fn node_stats(&self, node_id: &str) -> Option<NodeStats> {
        self.nodes.get(node_id).map(|state| {
            let state = state.read();
            NodeStats {
                sample_count: state.count,
                mean_interval_ms: state.mean_interval(&self.config),
                std_dev_ms: state.std_dev(&self.config),
                time_since_heartbeat_ms: Instant::now()
                    .duration_since(state.last_heartbeat)
                    .as_secs_f64()
                    * 1000.0,
                phi: state.compute_phi(&self.config),
            }
        })
    }
}

/// Statistics for a tracked node.
#[derive(Debug, Clone)]
pub struct NodeStats {
    /// Number of heartbeat samples recorded.
    pub sample_count: u64,
    /// Mean inter-arrival time in milliseconds.
    pub mean_interval_ms: f64,
    /// Standard deviation of inter-arrival times.
    pub std_dev_ms: f64,
    /// Time since last heartbeat in milliseconds.
    pub time_since_heartbeat_ms: f64,
    /// Current phi value.
    pub phi: f64,
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_detector_creation() {
        let detector = PhiAccrualDetector::with_defaults();
        assert_eq!(detector.node_count(), 0);
    }

    #[test]
    fn test_heartbeat_registers_node() {
        let detector = PhiAccrualDetector::with_defaults();
        detector.heartbeat("node-1");
        assert_eq!(detector.node_count(), 1);
        assert!(detector.phi("node-1").is_some());
    }

    #[test]
    fn test_unknown_node_returns_none() {
        let detector = PhiAccrualDetector::with_defaults();
        assert!(detector.phi("unknown").is_none());
    }

    #[test]
    fn test_healthy_after_heartbeat() {
        let detector = PhiAccrualDetector::with_defaults();
        detector.heartbeat("node-1");
        assert!(detector.is_healthy("node-1"));
        assert!(!detector.is_suspected("node-1"));
    }

    #[test]
    fn test_phi_increases_without_heartbeat() {
        let config = PhiDetectorConfig {
            heartbeat_interval_ms: 10, // Very short for testing
            threshold: 8.0,
            min_samples: 2,
            ..Default::default()
        };
        let detector = PhiAccrualDetector::new(config);

        // Send some heartbeats to establish baseline
        detector.heartbeat("node-1");
        thread::sleep(Duration::from_millis(10));
        detector.heartbeat("node-1");
        thread::sleep(Duration::from_millis(10));
        detector.heartbeat("node-1");

        let phi1 = detector.phi("node-1").unwrap();

        // Wait and check phi increased
        thread::sleep(Duration::from_millis(50));
        let phi2 = detector.phi("node-1").unwrap();

        assert!(phi2 > phi1, "phi should increase: {} > {}", phi2, phi1);
    }

    #[test]
    fn test_suspected_after_timeout() {
        let config = PhiDetectorConfig {
            heartbeat_interval_ms: 5,
            threshold: 2.0, // Low threshold for quick detection
            min_samples: 2,
            acceptable_delay_factor: 1.0,
            ..Default::default()
        };
        let detector = PhiAccrualDetector::new(config);

        // Establish baseline
        detector.heartbeat("node-1");
        thread::sleep(Duration::from_millis(5));
        detector.heartbeat("node-1");
        thread::sleep(Duration::from_millis(5));
        detector.heartbeat("node-1");

        // Wait long enough for timeout
        thread::sleep(Duration::from_millis(100));

        assert!(detector.is_suspected("node-1"));
    }

    #[test]
    fn test_remove_node() {
        let detector = PhiAccrualDetector::with_defaults();
        detector.heartbeat("node-1");
        assert_eq!(detector.node_count(), 1);

        detector.remove_node("node-1");
        assert_eq!(detector.node_count(), 0);
        assert!(detector.phi("node-1").is_none());
    }

    #[test]
    fn test_multiple_nodes() {
        let detector = PhiAccrualDetector::with_defaults();
        detector.heartbeat("node-1");
        detector.heartbeat("node-2");
        detector.heartbeat("node-3");

        assert_eq!(detector.node_count(), 3);
        assert!(detector.is_healthy("node-1"));
        assert!(detector.is_healthy("node-2"));
        assert!(detector.is_healthy("node-3"));
    }

    #[test]
    fn test_healthy_nodes_list() {
        let detector = PhiAccrualDetector::with_defaults();
        detector.heartbeat("node-1");
        detector.heartbeat("node-2");

        let healthy = detector.healthy_nodes();
        assert_eq!(healthy.len(), 2);
        assert!(healthy.contains(&"node-1".to_string()));
        assert!(healthy.contains(&"node-2".to_string()));
    }

    #[test]
    fn test_node_stats() {
        let detector = PhiAccrualDetector::with_defaults();
        detector.heartbeat("node-1");

        let stats = detector.node_stats("node-1").unwrap();
        assert_eq!(stats.sample_count, 1);
        assert!(stats.phi >= 0.0);
    }

    #[test]
    fn test_time_since_heartbeat() {
        let detector = PhiAccrualDetector::with_defaults();
        detector.heartbeat("node-1");

        thread::sleep(Duration::from_millis(10));

        let time = detector.time_since_heartbeat("node-1").unwrap();
        assert!(time >= Duration::from_millis(10));
    }

    #[test]
    fn test_erf_approximation() {
        // Test erf(0) = 0
        let result = NodeState::erf(0.0);
        assert!((result - 0.0).abs() < 0.01);

        // Test erf(1) ≈ 0.8427
        let result = NodeState::erf(1.0);
        assert!((result - 0.8427).abs() < 0.01);

        // Test erf is odd function
        let result_pos = NodeState::erf(0.5);
        let result_neg = NodeState::erf(-0.5);
        assert!((result_pos + result_neg).abs() < 0.001);
    }

    #[test]
    fn test_normal_cdf() {
        // CDF(0) = 0.5
        let result = NodeState::normal_cdf(0.0);
        assert!((result - 0.5).abs() < 0.01);

        // CDF(-∞) → 0, CDF(+∞) → 1
        let result = NodeState::normal_cdf(-5.0);
        assert!(result < 0.01);

        let result = NodeState::normal_cdf(5.0);
        assert!(result > 0.99);
    }

    #[test]
    fn test_concurrent_heartbeats() {
        use std::sync::Arc;

        let detector = Arc::new(PhiAccrualDetector::with_defaults());
        let mut handles = vec![];

        for i in 0..4 {
            let detector = Arc::clone(&detector);
            let node_id = format!("node-{i}");
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    detector.heartbeat(&node_id);
                    thread::sleep(Duration::from_micros(100));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(detector.node_count(), 4);
    }
}
