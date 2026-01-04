//! Chaos testing primitives for fault injection.
//!
//! Provides [`ChaosController`] for injecting network failures and [`ChaosNetwork`]
//! for wrapping Raft network implementations with controllable fault behavior.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::{DashMap, DashSet};
use rand::prelude::*;
use tokio::sync::RwLock;
use tracing::{debug, trace};

use crate::types::RaftNodeId;

/// Unique identifier for a network partition.
pub type PartitionId = u64;

/// Errors that can occur during chaos operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ChaosError {
    /// The target node is crashed.
    #[error("Node {0} is crashed")]
    NodeCrashed(RaftNodeId),

    /// Network partition prevents communication.
    #[error("Network partition between {0} and {1}")]
    Partitioned(RaftNodeId, RaftNodeId),

    /// Packet was dropped due to configured loss rate.
    #[error("Packet dropped (loss rate: {0:.1}%)")]
    PacketDropped(f64),

    /// Request timed out due to injected delay.
    #[error("Request timed out (delay: {0:?})")]
    Timeout(Duration),
}

/// Fault injection controller for chaos testing.
///
/// The controller maintains state about:
/// - Network delays between node pairs
/// - Packet loss rates between node pairs
/// - Network partitions (sets of isolated nodes)
/// - Crashed nodes
///
/// Use [`ChaosNetwork`] to wrap a Raft network and apply these faults.
///
/// # Thread Safety
///
/// All operations are thread-safe and can be called from any thread.
#[derive(Debug)]
pub struct ChaosController {
    /// Delay injection per node pair (source, target) -> delay.
    delays: DashMap<(RaftNodeId, RaftNodeId), Duration>,

    /// Packet drop rate per node pair (0.0 - 1.0).
    drop_rates: DashMap<(RaftNodeId, RaftNodeId), f64>,

    /// Partitioned node sets. Nodes in the same partition can communicate,
    /// but nodes in different partitions cannot.
    partitions: Arc<RwLock<HashMap<PartitionId, HashSet<RaftNodeId>>>>,

    /// Counter for generating partition IDs.
    partition_counter: AtomicU64,

    /// Crashed nodes (will reject all RPCs).
    crashed: DashSet<RaftNodeId>,

    /// RNG seed for reproducible failures.
    seed: u64,
}

impl ChaosController {
    /// Creates a new chaos controller with a random seed.
    #[must_use]
    pub fn new() -> Self {
        Self::with_seed(rand::thread_rng().gen())
    }

    /// Creates a new chaos controller with a specific seed for reproducibility.
    #[must_use]
    pub fn with_seed(seed: u64) -> Self {
        Self {
            delays: DashMap::new(),
            drop_rates: DashMap::new(),
            partitions: Arc::new(RwLock::new(HashMap::new())),
            partition_counter: AtomicU64::new(0),
            crashed: DashSet::new(),
            seed,
        }
    }

    /// Returns the RNG seed for reproducibility.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    // =========================================================================
    // Delay Injection
    // =========================================================================

    /// Injects network delay between two nodes.
    ///
    /// All messages from `from` to `to` will be delayed by `delay`.
    pub fn inject_delay(&self, from: RaftNodeId, to: RaftNodeId, delay: Duration) {
        debug!(from = from, to = to, delay_ms = delay.as_millis(), "Injecting delay");
        self.delays.insert((from, to), delay);
    }

    /// Injects symmetric network delay between two nodes.
    ///
    /// Messages in both directions will be delayed.
    pub fn inject_delay_symmetric(&self, node_a: RaftNodeId, node_b: RaftNodeId, delay: Duration) {
        self.inject_delay(node_a, node_b, delay);
        self.inject_delay(node_b, node_a, delay);
    }

    /// Removes delay injection between two nodes.
    pub fn clear_delay(&self, from: RaftNodeId, to: RaftNodeId) {
        self.delays.remove(&(from, to));
    }

    /// Removes all delay injections.
    pub fn clear_all_delays(&self) {
        self.delays.clear();
    }

    /// Gets the delay between two nodes, if any.
    #[must_use]
    pub fn get_delay(&self, from: RaftNodeId, to: RaftNodeId) -> Option<Duration> {
        self.delays.get(&(from, to)).map(|d| *d)
    }

    // =========================================================================
    // Packet Loss Injection
    // =========================================================================

    /// Injects packet loss between two nodes.
    ///
    /// `rate` should be between 0.0 (no loss) and 1.0 (100% loss).
    pub fn inject_packet_loss(&self, from: RaftNodeId, to: RaftNodeId, rate: f64) {
        let rate = rate.clamp(0.0, 1.0);
        debug!(from = from, to = to, rate = rate, "Injecting packet loss");
        self.drop_rates.insert((from, to), rate);
    }

    /// Injects symmetric packet loss between two nodes.
    pub fn inject_packet_loss_symmetric(&self, node_a: RaftNodeId, node_b: RaftNodeId, rate: f64) {
        self.inject_packet_loss(node_a, node_b, rate);
        self.inject_packet_loss(node_b, node_a, rate);
    }

    /// Removes packet loss injection between two nodes.
    pub fn clear_packet_loss(&self, from: RaftNodeId, to: RaftNodeId) {
        self.drop_rates.remove(&(from, to));
    }

    /// Removes all packet loss injections.
    pub fn clear_all_packet_loss(&self) {
        self.drop_rates.clear();
    }

    /// Gets the packet loss rate between two nodes.
    #[must_use]
    pub fn get_packet_loss(&self, from: RaftNodeId, to: RaftNodeId) -> f64 {
        self.drop_rates.get(&(from, to)).map(|r| *r).unwrap_or(0.0)
    }

    // =========================================================================
    // Network Partitions
    // =========================================================================

    /// Creates a network partition isolating a set of nodes.
    ///
    /// Nodes in the partition can communicate with each other,
    /// but cannot communicate with nodes outside the partition.
    ///
    /// Returns the partition ID for later reference.
    pub async fn partition(&self, isolated: HashSet<RaftNodeId>) -> PartitionId {
        let id = self.partition_counter.fetch_add(1, Ordering::SeqCst);
        debug!(partition_id = id, nodes = ?isolated, "Creating network partition");
        self.partitions.write().await.insert(id, isolated);
        id
    }

    /// Heals a specific partition by ID.
    pub async fn heal_partition(&self, id: PartitionId) {
        debug!(partition_id = id, "Healing partition");
        self.partitions.write().await.remove(&id);
    }

    /// Heals all network partitions.
    pub async fn heal_all(&self) {
        debug!("Healing all partitions");
        self.partitions.write().await.clear();
    }

    /// Checks if two nodes can communicate (not separated by a partition).
    pub async fn can_communicate(&self, from: RaftNodeId, to: RaftNodeId) -> bool {
        let partitions = self.partitions.read().await;

        // If no partitions, everyone can communicate
        if partitions.is_empty() {
            return true;
        }

        // Find which partition each node belongs to
        let from_partition = partitions.iter().find(|(_, nodes)| nodes.contains(&from));
        let to_partition = partitions.iter().find(|(_, nodes)| nodes.contains(&to));

        match (from_partition, to_partition) {
            // Both in same partition - can communicate
            (Some((id1, _)), Some((id2, _))) if id1 == id2 => true,
            // Both in different partitions - cannot communicate
            (Some(_), Some(_)) => false,
            // One or both not in any partition - treat as separate partition
            // If from is isolated and to is not, they can't communicate
            (Some(_), None) | (None, Some(_)) => false,
            // Neither in a partition - can communicate
            (None, None) => true,
        }
    }

    // =========================================================================
    // Node Crashes
    // =========================================================================

    /// Simulates a node crash.
    ///
    /// The crashed node will reject all incoming and outgoing RPCs.
    pub fn crash_node(&self, node: RaftNodeId) {
        debug!(node = node, "Crashing node");
        self.crashed.insert(node);
    }

    /// Recovers a crashed node.
    pub fn recover_node(&self, node: RaftNodeId) {
        debug!(node = node, "Recovering node");
        self.crashed.remove(&node);
    }

    /// Checks if a node is crashed.
    #[must_use]
    pub fn is_crashed(&self, node: RaftNodeId) -> bool {
        self.crashed.contains(&node)
    }

    // =========================================================================
    // RPC Interception
    // =========================================================================

    /// Intercepts an RPC and applies configured chaos.
    ///
    /// Returns `Ok(delay)` if the RPC should proceed (with optional delay),
    /// or `Err(ChaosError)` if the RPC should fail.
    pub async fn intercept(
        &self,
        from: RaftNodeId,
        to: RaftNodeId,
    ) -> Result<Option<Duration>, ChaosError> {
        trace!(from = from, to = to, "Intercepting RPC");

        // Check if either node is crashed
        if self.is_crashed(from) {
            return Err(ChaosError::NodeCrashed(from));
        }
        if self.is_crashed(to) {
            return Err(ChaosError::NodeCrashed(to));
        }

        // Check for network partition
        if !self.can_communicate(from, to).await {
            return Err(ChaosError::Partitioned(from, to));
        }

        // Check for packet loss
        if let Some(rate) = self.drop_rates.get(&(from, to)) {
            let mut rng = StdRng::seed_from_u64(self.seed);
            if rng.gen::<f64>() < *rate {
                return Err(ChaosError::PacketDropped(*rate * 100.0));
            }
        }

        // Get delay if configured
        let delay = self.get_delay(from, to);

        Ok(delay)
    }

    // =========================================================================
    // Bulk Operations
    // =========================================================================

    /// Resets all chaos state.
    pub async fn reset(&self) {
        self.delays.clear();
        self.drop_rates.clear();
        self.partitions.write().await.clear();
        self.crashed.clear();
    }

    /// Creates a complete network partition isolating each node from all others.
    ///
    /// This simulates a complete network failure where no nodes can communicate.
    pub async fn total_partition(&self, nodes: &[RaftNodeId]) {
        for node in nodes {
            let _ = self.partition(HashSet::from([*node])).await;
        }
    }

    /// Injects random jitter on all links.
    ///
    /// Each link gets a random delay between `min` and `max`.
    pub fn inject_random_jitter(&self, nodes: &[RaftNodeId], min: Duration, max: Duration) {
        let mut rng = StdRng::seed_from_u64(self.seed);
        let range = max - min;

        for &from in nodes {
            for &to in nodes {
                if from != to {
                    let jitter = min + range.mul_f64(rng.gen());
                    self.inject_delay(from, to, jitter);
                }
            }
        }
    }
}

impl Default for ChaosController {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chaos_controller_new() {
        let controller = ChaosController::new();
        assert!(!controller.is_crashed(1));
        assert_eq!(controller.get_delay(1, 2), None);
        assert_eq!(controller.get_packet_loss(1, 2), 0.0);
    }

    #[test]
    fn test_delay_injection() {
        let controller = ChaosController::new();

        controller.inject_delay(1, 2, Duration::from_millis(100));
        assert_eq!(controller.get_delay(1, 2), Some(Duration::from_millis(100)));
        assert_eq!(controller.get_delay(2, 1), None);

        controller.inject_delay_symmetric(3, 4, Duration::from_millis(50));
        assert_eq!(controller.get_delay(3, 4), Some(Duration::from_millis(50)));
        assert_eq!(controller.get_delay(4, 3), Some(Duration::from_millis(50)));

        controller.clear_delay(1, 2);
        assert_eq!(controller.get_delay(1, 2), None);

        controller.clear_all_delays();
        assert_eq!(controller.get_delay(3, 4), None);
    }

    #[test]
    fn test_packet_loss_injection() {
        let controller = ChaosController::new();

        controller.inject_packet_loss(1, 2, 0.5);
        assert_eq!(controller.get_packet_loss(1, 2), 0.5);
        assert_eq!(controller.get_packet_loss(2, 1), 0.0);

        // Test clamping
        controller.inject_packet_loss(1, 3, 1.5);
        assert_eq!(controller.get_packet_loss(1, 3), 1.0);

        controller.inject_packet_loss(1, 4, -0.5);
        assert_eq!(controller.get_packet_loss(1, 4), 0.0);
    }

    #[test]
    fn test_node_crash() {
        let controller = ChaosController::new();

        assert!(!controller.is_crashed(1));
        controller.crash_node(1);
        assert!(controller.is_crashed(1));
        controller.recover_node(1);
        assert!(!controller.is_crashed(1));
    }

    #[tokio::test]
    async fn test_partition() {
        let controller = ChaosController::new();

        // Initially everyone can communicate
        assert!(controller.can_communicate(1, 2).await);
        assert!(controller.can_communicate(1, 3).await);

        // Create partition isolating node 1
        let partition_id = controller.partition(HashSet::from([1])).await;
        assert!(!controller.can_communicate(1, 2).await);
        assert!(!controller.can_communicate(2, 1).await);

        // Heal the partition
        controller.heal_partition(partition_id).await;
        assert!(controller.can_communicate(1, 2).await);
    }

    #[tokio::test]
    async fn test_intercept_crashed_node() {
        let controller = ChaosController::new();

        controller.crash_node(2);

        let result = controller.intercept(1, 2).await;
        assert!(matches!(result, Err(ChaosError::NodeCrashed(2))));

        let result = controller.intercept(2, 1).await;
        assert!(matches!(result, Err(ChaosError::NodeCrashed(2))));
    }

    #[tokio::test]
    async fn test_intercept_partition() {
        let controller = ChaosController::new();

        controller.partition(HashSet::from([1])).await;

        let result = controller.intercept(1, 2).await;
        assert!(matches!(result, Err(ChaosError::Partitioned(1, 2))));
    }

    #[tokio::test]
    async fn test_intercept_delay() {
        let controller = ChaosController::new();

        controller.inject_delay(1, 2, Duration::from_millis(100));

        let result = controller.intercept(1, 2).await;
        assert_eq!(result.unwrap(), Some(Duration::from_millis(100)));
    }

    #[tokio::test]
    async fn test_reset() {
        let controller = ChaosController::new();

        controller.inject_delay(1, 2, Duration::from_millis(100));
        controller.inject_packet_loss(1, 3, 0.5);
        controller.crash_node(4);
        controller.partition(HashSet::from([5])).await;

        controller.reset().await;

        assert_eq!(controller.get_delay(1, 2), None);
        assert_eq!(controller.get_packet_loss(1, 3), 0.0);
        assert!(!controller.is_crashed(4));
        assert!(controller.can_communicate(5, 1).await);
    }
}
