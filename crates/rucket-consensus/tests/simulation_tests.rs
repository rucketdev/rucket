//! Deterministic simulation tests using Turmoil.
//!
//! These tests run the Raft cluster in a simulated environment where:
//! - Time is virtual and can be fast-forwarded
//! - Network behavior is fully controlled and reproducible
//! - Tests run single-threaded for determinism
//!
//! Run with: `cargo test --test simulation_tests`

use std::time::Duration;

use turmoil::Builder;

// =============================================================================
// Basic Turmoil Simulation Tests
// =============================================================================

/// Test that basic Turmoil simulation works.
#[test]
fn test_turmoil_basic_simulation() {
    let mut sim = Builder::new().build();

    // Register a simple host
    sim.host("server", || async {
        // Simulate some work
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    });

    // Run the simulation
    sim.run().unwrap();
}

/// Test host-to-host communication.
#[test]
fn test_turmoil_host_communication() {
    let mut sim = Builder::new().build();

    // Server host that listens for connections
    sim.host("server", || async {
        let listener = turmoil::net::TcpListener::bind("0.0.0.0:8080").await?;
        let (mut socket, _) = listener.accept().await?;

        // Read message
        let mut buf = [0u8; 1024];
        let n = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await?;
        assert_eq!(&buf[..n], b"hello");

        // Send response
        tokio::io::AsyncWriteExt::write_all(&mut socket, b"world").await?;

        Ok(())
    });

    // Client host that connects to server
    sim.client("client", async {
        // Wait for server to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        let addr = turmoil::lookup("server");
        let mut socket = turmoil::net::TcpStream::connect((addr, 8080)).await?;

        // Send message
        tokio::io::AsyncWriteExt::write_all(&mut socket, b"hello").await?;

        // Read response
        let mut buf = [0u8; 1024];
        let n = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await?;
        assert_eq!(&buf[..n], b"world");

        Ok(())
    });

    sim.run().unwrap();
}

/// Test network partition simulation.
#[test]
fn test_turmoil_network_partition() {
    let mut sim = Builder::new().build();

    sim.host("node1", || async {
        let listener = turmoil::net::TcpListener::bind("0.0.0.0:9001").await?;

        // Accept connection before partition
        let (mut socket, _) = listener.accept().await?;
        let mut buf = [0u8; 1024];
        let n = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await?;
        assert_eq!(&buf[..n], b"before_partition");

        // After partition heals, accept another connection
        let (mut socket2, _) = listener.accept().await?;
        let n = tokio::io::AsyncReadExt::read(&mut socket2, &mut buf).await?;
        assert_eq!(&buf[..n], b"after_partition");

        Ok(())
    });

    sim.client("controller", async {
        let addr = turmoil::lookup("node1");

        // Connect before partition
        let mut socket = turmoil::net::TcpStream::connect((addr, 9001)).await?;
        tokio::io::AsyncWriteExt::write_all(&mut socket, b"before_partition").await?;
        drop(socket);

        // Create partition
        turmoil::partition("controller", "node1");

        // Wait while partitioned (connection attempts would fail)
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Repair partition
        turmoil::repair("controller", "node1");

        // Connect after repair
        let mut socket = turmoil::net::TcpStream::connect((addr, 9001)).await?;
        tokio::io::AsyncWriteExt::write_all(&mut socket, b"after_partition").await?;

        Ok(())
    });

    sim.run().unwrap();
}

/// Test time advancement in simulation.
#[test]
fn test_turmoil_time_simulation() {
    let mut sim = Builder::new().build();

    sim.client("test", async {
        let start = turmoil::elapsed();

        // Sleep for 1 second (virtual time)
        tokio::time::sleep(Duration::from_secs(1)).await;

        let elapsed = turmoil::elapsed() - start;
        assert!(elapsed >= Duration::from_secs(1));
        assert!(elapsed < Duration::from_secs(2));

        Ok(())
    });

    sim.run().unwrap();
}

/// Test multiple hosts with message passing.
#[test]
fn test_turmoil_multi_host_messaging() {
    let mut sim = Builder::new().build();

    // Create 3 nodes
    for i in 1..=3 {
        let node_name = format!("node{}", i);
        sim.host(node_name, || async move {
            let listener = turmoil::net::TcpListener::bind("0.0.0.0:9001").await?;

            // Accept one connection from each other node
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await?;
                let mut buf = [0u8; 1024];
                let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await?;
            }

            Ok(())
        });
    }

    sim.client("coordinator", async {
        // Give nodes time to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Each node sends to every other node
        for i in 1..=3 {
            for j in 1..=3 {
                if i != j {
                    let target = format!("node{}", j);
                    let addr = turmoil::lookup(target.as_str());
                    if let Ok(mut socket) = turmoil::net::TcpStream::connect((addr, 9001)).await {
                        let msg = format!("hello from node{}", i);
                        let _ =
                            tokio::io::AsyncWriteExt::write_all(&mut socket, msg.as_bytes()).await;
                    }
                }
            }
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// Test that partitions prevent communication.
#[test]
fn test_turmoil_partition_blocks_communication() {
    let mut sim = Builder::new().simulation_duration(Duration::from_secs(5)).build();

    sim.host("isolated", || async {
        let listener = turmoil::net::TcpListener::bind("0.0.0.0:9001").await?;

        // This accept should timeout because we're partitioned
        if tokio::time::timeout(Duration::from_secs(2), listener.accept()).await.is_ok() {
            panic!("Should not have received connection while partitioned");
        }
        // Expected timeout if Err

        Ok(())
    });

    sim.client("controller", async {
        // Partition immediately
        turmoil::partition("controller", "isolated");

        // Try to connect (should fail or timeout)
        let addr = turmoil::lookup("isolated");
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            turmoil::net::TcpStream::connect((addr, 9001)),
        )
        .await;

        // Connection should fail or timeout
        assert!(result.is_err() || result.unwrap().is_err());

        // Let isolated node's timeout complete
        tokio::time::sleep(Duration::from_secs(3)).await;

        Ok(())
    });

    sim.run().unwrap();
}

/// Test message holding and release.
#[test]
fn test_turmoil_hold_and_release() {
    let mut sim = Builder::new().build();

    sim.host("receiver", || async {
        let listener = turmoil::net::TcpListener::bind("0.0.0.0:9001").await?;
        let (mut socket, _) = listener.accept().await?;

        let mut buf = [0u8; 1024];
        let n = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await?;
        assert_eq!(&buf[..n], b"delayed_message");

        Ok(())
    });

    sim.client("sender", async {
        tokio::time::sleep(Duration::from_millis(10)).await;

        let addr = turmoil::lookup("receiver");
        let mut socket = turmoil::net::TcpStream::connect((addr, 9001)).await?;

        // Hold messages
        turmoil::hold("sender", "receiver");

        // Send message (will be held)
        tokio::io::AsyncWriteExt::write_all(&mut socket, b"delayed_message").await?;

        // Wait a bit
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Release held messages
        turmoil::release("sender", "receiver");

        // Give time for message to be delivered
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(())
    });

    sim.run().unwrap();
}

// =============================================================================
// Raft-like Behavior Tests
// =============================================================================

/// Simulates basic leader election timeout behavior.
#[test]
fn test_simulated_election_timeout() {
    let mut sim = Builder::new().build();

    sim.client("test", async {
        // Simulate election timeout (150-300ms in real Raft)
        let election_timeout = Duration::from_millis(200);
        let heartbeat_interval = Duration::from_millis(50);

        // Simulate follower waiting for heartbeat
        let start = turmoil::elapsed();

        // No heartbeat received, election timeout fires
        tokio::time::sleep(election_timeout).await;

        let elapsed = turmoil::elapsed() - start;
        assert!(elapsed >= election_timeout);

        // After becoming candidate, election would proceed
        // Simulate winning election and sending heartbeats
        for _ in 0..5 {
            tokio::time::sleep(heartbeat_interval).await;
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// Test simulated cluster with leader and followers.
#[test]
fn test_simulated_leader_follower() {
    let mut sim = Builder::new().build();

    // Leader sends heartbeats
    sim.host("leader", || async {
        let listener = turmoil::net::TcpListener::bind("0.0.0.0:9001").await?;

        // Accept connections from followers
        let mut followers = Vec::new();
        for _ in 0..2 {
            let (socket, _) = listener.accept().await?;
            followers.push(socket);
        }

        // Send heartbeats
        for _ in 0..5 {
            for follower in &mut followers {
                let _ = tokio::io::AsyncWriteExt::write_all(follower, b"heartbeat").await;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(())
    });

    // Followers connect to leader and receive heartbeats
    for i in 1..=2 {
        let follower_name = format!("follower{}", i);
        sim.host(follower_name, || async move {
            tokio::time::sleep(Duration::from_millis(10)).await;

            let leader_addr = turmoil::lookup("leader");
            let mut socket = turmoil::net::TcpStream::connect((leader_addr, 9001)).await?;

            // Receive heartbeats
            let mut heartbeats = 0;
            let mut buf = [0u8; 1024];
            while heartbeats < 3 {
                match tokio::time::timeout(
                    Duration::from_millis(100),
                    tokio::io::AsyncReadExt::read(&mut socket, &mut buf),
                )
                .await
                {
                    Ok(Ok(n)) if n > 0 => {
                        heartbeats += 1;
                    }
                    _ => break,
                }
            }

            assert!(heartbeats >= 3, "Should have received at least 3 heartbeats");

            Ok(())
        });
    }

    sim.run().unwrap();
}

/// Test leader failure and re-election simulation.
#[test]
fn test_simulated_leader_failure() {
    let mut sim = Builder::new().simulation_duration(Duration::from_secs(10)).build();

    sim.host("leader", || async {
        let listener = turmoil::net::TcpListener::bind("0.0.0.0:9001").await?;

        // Accept follower connection
        let (mut socket, _) = listener.accept().await?;

        // Send a few heartbeats then "crash"
        for _ in 0..3 {
            let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, b"heartbeat").await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Leader crashes (stops sending heartbeats)
        // In real scenario, the process would exit
        tokio::time::sleep(Duration::from_secs(5)).await;

        Ok(())
    });

    sim.host("follower", || async {
        tokio::time::sleep(Duration::from_millis(10)).await;

        let leader_addr = turmoil::lookup("leader");
        let mut socket = turmoil::net::TcpStream::connect((leader_addr, 9001)).await?;

        let election_timeout = Duration::from_millis(200);
        let mut last_heartbeat = turmoil::elapsed();

        loop {
            let mut buf = [0u8; 1024];
            match tokio::time::timeout(
                election_timeout,
                tokio::io::AsyncReadExt::read(&mut socket, &mut buf),
            )
            .await
            {
                Ok(Ok(n)) if n > 0 => {
                    last_heartbeat = turmoil::elapsed();
                }
                _ => {
                    // No heartbeat received - would trigger election
                    let time_since_heartbeat = turmoil::elapsed() - last_heartbeat;
                    if time_since_heartbeat >= election_timeout {
                        // Election would start here
                        break;
                    }
                }
            }
        }

        Ok(())
    });

    sim.run().unwrap();
}

// =============================================================================
// Determinism Tests
// =============================================================================

/// Verify that simulations are deterministic with the same seed.
#[test]
fn test_simulation_determinism() {
    fn run_simulation() -> Vec<Duration> {
        let mut sim = Builder::new().build();
        let timestamps = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let ts_clone = timestamps.clone();

        sim.client("test", async move {
            for i in 0..5 {
                let delay = Duration::from_millis(10 * (i + 1));
                tokio::time::sleep(delay).await;
                ts_clone.lock().unwrap().push(turmoil::elapsed());
            }
            Ok(())
        });

        sim.run().unwrap();
        std::sync::Arc::try_unwrap(timestamps).unwrap().into_inner().unwrap()
    }

    // Run twice and compare
    let run1 = run_simulation();
    let run2 = run_simulation();

    assert_eq!(run1, run2, "Simulations should be deterministic");
}
