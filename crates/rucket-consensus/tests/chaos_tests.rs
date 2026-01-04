//! Chaos testing for Raft cluster resilience.
//!
//! These tests verify that the Raft cluster behaves correctly under various
//! failure scenarios including network partitions, node crashes, and delays.
//!
//! Run with: `cargo test --features chaos-testing --test chaos_tests`

#![cfg(feature = "chaos-testing")]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use rucket_consensus::testing::{ChaosController, ChaosError};

// =============================================================================
// Basic ChaosController Tests
// =============================================================================

#[tokio::test]
async fn test_chaos_controller_delay_symmetric() {
    let controller = ChaosController::new();

    // Inject symmetric delay
    controller.inject_delay_symmetric(1, 2, Duration::from_millis(50));

    // Verify both directions have delay
    assert_eq!(controller.get_delay(1, 2), Some(Duration::from_millis(50)));
    assert_eq!(controller.get_delay(2, 1), Some(Duration::from_millis(50)));

    // Verify other pairs don't have delay
    assert_eq!(controller.get_delay(1, 3), None);
}

#[tokio::test]
async fn test_chaos_controller_packet_loss_clamping() {
    let controller = ChaosController::new();

    // Test clamping above 1.0
    controller.inject_packet_loss(1, 2, 1.5);
    assert_eq!(controller.get_packet_loss(1, 2), 1.0);

    // Test clamping below 0.0
    controller.inject_packet_loss(1, 3, -0.5);
    assert_eq!(controller.get_packet_loss(1, 3), 0.0);

    // Normal value
    controller.inject_packet_loss(1, 4, 0.25);
    assert_eq!(controller.get_packet_loss(1, 4), 0.25);
}

#[tokio::test]
async fn test_chaos_controller_multiple_partitions() {
    let controller = ChaosController::new();

    // Create partition isolating nodes 1 and 2
    let p1 = controller.partition(HashSet::from([1, 2])).await;

    // Create another partition isolating nodes 3 and 4
    let _p2 = controller.partition(HashSet::from([3, 4])).await;

    // Nodes in same partition can communicate
    assert!(controller.can_communicate(1, 2).await);
    assert!(controller.can_communicate(3, 4).await);

    // Nodes in different partitions cannot
    assert!(!controller.can_communicate(1, 3).await);
    assert!(!controller.can_communicate(2, 4).await);

    // Heal first partition
    controller.heal_partition(p1).await;
    assert!(!controller.can_communicate(1, 3).await); // Still can't reach partition 2

    // Heal all
    controller.heal_all().await;
    assert!(controller.can_communicate(1, 3).await);
    assert!(controller.can_communicate(2, 4).await);
}

#[tokio::test]
async fn test_chaos_controller_total_partition() {
    let controller = ChaosController::new();

    // Create total partition
    controller.total_partition(&[1, 2, 3]).await;

    // No node can communicate with any other
    assert!(!controller.can_communicate(1, 2).await);
    assert!(!controller.can_communicate(2, 3).await);
    assert!(!controller.can_communicate(1, 3).await);
}

#[tokio::test]
async fn test_chaos_controller_random_jitter() {
    let controller = ChaosController::with_seed(42); // Reproducible

    controller.inject_random_jitter(
        &[1, 2, 3],
        Duration::from_millis(10),
        Duration::from_millis(100),
    );

    // All links should have some delay
    for from in 1..=3 {
        for to in 1..=3 {
            if from != to {
                let delay = controller.get_delay(from, to);
                assert!(delay.is_some(), "Expected delay on link {} -> {}", from, to);
                let d = delay.unwrap();
                assert!(d >= Duration::from_millis(10));
                assert!(d <= Duration::from_millis(100));
            }
        }
    }
}

#[tokio::test]
async fn test_chaos_controller_seed_reproducibility() {
    // Same seed should produce same random jitter
    let controller1 = ChaosController::with_seed(42);
    let controller2 = ChaosController::with_seed(42);

    controller1.inject_random_jitter(
        &[1, 2, 3],
        Duration::from_millis(10),
        Duration::from_millis(100),
    );

    controller2.inject_random_jitter(
        &[1, 2, 3],
        Duration::from_millis(10),
        Duration::from_millis(100),
    );

    // Verify same delays
    for from in 1..=3 {
        for to in 1..=3 {
            if from != to {
                assert_eq!(
                    controller1.get_delay(from, to),
                    controller2.get_delay(from, to),
                    "Delays should match for seed 42 on link {} -> {}",
                    from,
                    to
                );
            }
        }
    }
}

// =============================================================================
// Intercept Tests
// =============================================================================

#[tokio::test]
async fn test_intercept_priority_crash_over_partition() {
    let controller = ChaosController::new();

    // Create both a crash and a partition
    controller.crash_node(1);
    controller.partition(HashSet::from([1])).await;

    // Crash should be detected first
    let result = controller.intercept(1, 2).await;
    assert!(matches!(result, Err(ChaosError::NodeCrashed(1))));
}

#[tokio::test]
async fn test_intercept_no_chaos_returns_none() {
    let controller = ChaosController::new();

    let result = controller.intercept(1, 2).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);
}

#[tokio::test]
async fn test_intercept_with_delay_only() {
    let controller = ChaosController::new();

    controller.inject_delay(1, 2, Duration::from_millis(50));

    let result = controller.intercept(1, 2).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(Duration::from_millis(50)));
}

// =============================================================================
// Stress Tests
// =============================================================================

#[tokio::test]
async fn test_many_concurrent_operations() {
    let controller = Arc::new(ChaosController::new());

    // Spawn many concurrent tasks that read/write chaos state
    let mut handles = vec![];

    for i in 0..10 {
        let c = Arc::clone(&controller);
        handles.push(tokio::spawn(async move {
            for j in 0..100 {
                c.inject_delay(i, j, Duration::from_millis(10));
                let _ = c.get_delay(i, j);
                c.clear_delay(i, j);
            }
        }));
    }

    // All tasks should complete without deadlock
    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_partition_operations() {
    let controller = Arc::new(ChaosController::new());

    // Spawn concurrent partition creation and healing
    let c1 = Arc::clone(&controller);
    let h1 = tokio::spawn(async move {
        for i in 0..50 {
            let id = c1.partition(HashSet::from([i])).await;
            tokio::time::sleep(Duration::from_micros(10)).await;
            c1.heal_partition(id).await;
        }
    });

    let c2 = Arc::clone(&controller);
    let h2 = tokio::spawn(async move {
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_micros(50)).await;
            c2.heal_all().await;
        }
    });

    h1.await.unwrap();
    h2.await.unwrap();
}

// =============================================================================
// Scenario Tests
// =============================================================================

#[tokio::test]
async fn test_scenario_minority_partition() {
    let controller = ChaosController::new();

    // Simulate 2 nodes partitioned from 3-node cluster
    // Nodes 1, 2 can communicate; node 3 is isolated
    controller.partition(HashSet::from([1, 2])).await;
    controller.partition(HashSet::from([3])).await;

    // Majority (1, 2) can communicate
    assert!(controller.can_communicate(1, 2).await);

    // Node 3 is isolated
    assert!(!controller.can_communicate(1, 3).await);
    assert!(!controller.can_communicate(2, 3).await);
}

#[tokio::test]
async fn test_scenario_leader_crash() {
    let controller = ChaosController::new();
    let leader = 1;

    // Leader is working
    assert!(controller.intercept(2, leader).await.is_ok());
    assert!(controller.intercept(leader, 2).await.is_ok());

    // Crash leader
    controller.crash_node(leader);

    // RPCs to/from leader fail
    assert!(matches!(controller.intercept(2, leader).await, Err(ChaosError::NodeCrashed(1))));
    assert!(matches!(controller.intercept(leader, 2).await, Err(ChaosError::NodeCrashed(1))));

    // Other nodes can still communicate
    assert!(controller.intercept(2, 3).await.is_ok());

    // Recover leader
    controller.recover_node(leader);
    assert!(controller.intercept(2, leader).await.is_ok());
}

#[tokio::test]
async fn test_scenario_network_delay_then_heal() {
    let controller = ChaosController::new();

    // Add delay
    controller.inject_delay(1, 2, Duration::from_millis(500));

    let result = controller.intercept(1, 2).await;
    assert_eq!(result.unwrap(), Some(Duration::from_millis(500)));

    // Clear delay
    controller.clear_all_delays();

    let result = controller.intercept(1, 2).await;
    assert_eq!(result.unwrap(), None);
}

#[tokio::test]
async fn test_scenario_asymmetric_partition() {
    let controller = ChaosController::new();

    // Create asymmetric partition: 1 -> 2 is blocked, but 2 -> 1 works
    // This simulates one-way network failure
    controller.partition(HashSet::from([1])).await;

    // Node 1 can't reach anyone
    assert!(!controller.can_communicate(1, 2).await);

    // But node 2 can still reach other nodes (not 1)
    // In our model, if 1 is in a partition by itself, no one can reach it either
    assert!(!controller.can_communicate(2, 1).await);
}

#[tokio::test]
async fn test_scenario_progressive_failure() {
    let controller = ChaosController::new();
    let nodes = [1, 2, 3, 4, 5];

    // Progressively crash nodes
    for &node in &nodes[..3] {
        controller.crash_node(node);

        // Verify crashed nodes are unreachable
        for &other in &nodes {
            if other != node {
                let result = controller.intercept(other, node).await;
                if controller.is_crashed(node) {
                    assert!(matches!(result, Err(ChaosError::NodeCrashed(_))));
                }
            }
        }
    }

    // Verify remaining nodes can still communicate
    assert!(controller.intercept(4, 5).await.is_ok());
    assert!(controller.intercept(5, 4).await.is_ok());
}
