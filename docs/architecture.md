# Rucket Architecture Design

## Goal

Design a forward-compatible architecture where each phase builds naturally on the previous without retrofitting.

```
Phase 1: S3 Parity     → Single-node, full S3 API
Phase 2: Production    → Object Lock, SSE, hardened durability
Phase 3: Distributed   → Clustering, erasure coding, HA
Phase 4: Geo           → Cross-region replication
Phase 5: Governance    → Community, foundation
```

---

## The Actually Hard Problems

These are the fundamental distributed systems challenges that simple "add a field" doesn't solve:

### Problem 1: Consistency Model (CAP)

**Decision required:** Is Rucket CP or AP during network partitions?

| Choice | Behavior | Trade-off |
|--------|----------|-----------|
| CP (Consistent) | Reject writes during partition | Unavailability, but no conflicts |
| AP (Available) | Accept writes, merge later | Available, but potential data loss/conflicts |

**Recommendation:** AP with configurable consistency per bucket.
- Default: Eventual consistency (AP) - matches S3 behavior
- Optional: Strong consistency for critical buckets
- Rationale: S3 itself is eventually consistent for overwrites

**What this means for implementation:**
- Write path must be partition-tolerant
- Conflict resolution strategy must be deterministic
- Clients must handle stale reads

### Problem 2: Metadata Architecture

**Decision required:** How is metadata distributed?

| Option | Scalability | Complexity | Failure Blast Radius |
|--------|-------------|------------|---------------------|
| Single Raft group | ~100M objects | Low | Entire cluster |
| Raft per PG | Billions | High | Single PG |
| Separate metadata cluster | Billions | Very High | Metadata layer |

**Recommendation:** Start with single Raft, design for Raft-per-PG.

**Phase 3 approach:**
1. Single Raft group for metadata (simple start)
2. Shard data by PG, but metadata still centralized
3. Scale limit: ~100M objects (good for SMB)

**Phase 3.5 (if needed):**
1. Migrate to Raft-per-PG
2. Each PG has its own Raft group
3. Metadata operations route to PG owner

**What this means for Phase 1:**
- Object keys should be designed to support sharding (hash-based PG assignment)
- Bucket metadata vs object metadata should be separated
- Cross-bucket operations should be avoided in hot paths

### Problem 3: Write Path Design

**Decision required:** How do writes flow through the system?

**Recommendation:** Primary-Backup with configurable durability.

```
Durability Level: "local"
  Client → Local Node → Ack (no replication)

Durability Level: "replicated" (default)
  Client → Primary → Write locally → Ack
           ↓ (async)
       [Backup1, Backup2]

Durability Level: "durable"
  Client → Primary → Write locally
           ↓ (sync)
       [Backup1, Backup2] → Quorum Ack → Client Ack
```

**Why Primary-Backup over Chain Replication:**
- Lower latency for common case (async replication)
- Simpler failure handling
- S3 behavior is eventually consistent anyway

**What this means for Phase 1:**
- WAL events must be structured for replication (already planned)
- Write acknowledgment semantics must be defined in API
- Idempotency keys should be supported (x-amz-content-sha256 helps)

### Problem 4: Failure Detection & Recovery

**Decision required:** How do we detect and handle failures?

**Recommendation:** Phi Accrual Failure Detector + Raft leader election.

**Failure detection:**
- Heartbeats between nodes (every 1s)
- Phi accrual detector with threshold ~8 (adaptive timeout)
- Suspected nodes get 30s grace period before declared dead

**Recovery strategy:**
- Node failure: Raft elects new leader, backups promoted
- Disk failure: Rebuild from replicas/erasure shards
- Region failure: Promote replica region to primary

**What this means for Phase 1:**
- Health check endpoints (already have /minio/health/*)
- Graceful shutdown with drain period
- State should be recoverable from WAL

### Problem 5: Geo-Replication Conflict Resolution

**Decision required:** What happens when two regions write the same key?

**Recommendation:** Last-Writer-Wins (LWW) with HLC + Version History.

**Why LWW:**
- Simple to implement and reason about
- Deterministic (no manual conflict resolution)
- Matches S3 behavior (last PUT wins)

**Why keep version history:**
- Versioned buckets already keep all versions
- Even non-versioned: keep last N versions for conflict recovery
- Admin can inspect and manually recover if needed

**Conflict resolution algorithm:**
```rust
fn resolve_conflict(local: &Object, remote: &Object) -> ConflictResolution {
    match local.hlc.cmp(&remote.hlc) {
        Greater => KeepLocal,
        Less => AcceptRemote,
        Equal => {
            // Tie-breaker: higher region ID wins (deterministic)
            if remote.home_region > local.home_region {
                AcceptRemote
            } else {
                KeepLocal
            }
        }
    }
}
```

**What this means for Phase 1:**
- HLC must be assigned on every write
- Version history infrastructure (already exists)
- Region ID must be stable and globally unique

### Problem 6: Exactly-Once Semantics

**Decision required:** How do we handle duplicate requests?

**Recommendation:** Content-based deduplication + optional idempotency keys.

**Strategy 1: Content-addressed storage**
- ETag (MD5/SHA256) already computed
- Same content = same storage location
- PUT with same content is naturally idempotent

**Strategy 2: Idempotency keys (future)**
- Client sends `x-amz-idempotency-key: <uuid>`
- Server stores key → result mapping for 24h
- Retry with same key returns cached result

**What this means for Phase 1:**
- ETag computation is critical (already done)
- Consider storing request hashes for deduplication window
- API should document idempotency behavior

---

## Core Deployment Principle: Single Binary Simplicity

**The entire distributed system MUST be a single binary with minimal configuration.**

### Deployment Examples

```bash
# Phase 1: Single node - just works
rucket serve

# Phase 3: 3-node cluster - one flag
rucket serve --cluster --peers node2:9000,node3:9000

# Phase 3: With service discovery (DNS)
rucket serve --cluster --discover dns:///rucket.local

# Phase 3: Kubernetes (auto-discovery via headless service)
rucket serve --cluster --discover k8s:///rucket-headless.default.svc

# Phase 4: Geo-distributed
rucket serve --cluster --region us-east --geo-peers eu-west.example.com
```

### Minimal Configuration
```toml
[server]
bind = "0.0.0.0:9000"

[cluster]
enabled = true
peers = ["node2:9000", "node3:9000"]  # OR discover = "dns:///..."

[storage]
data_dir = "/var/lib/rucket"
```

### What's Inside the Single Binary
- HTTP/S3 API server (hyper/axum)
- Raft consensus engine (openraft or custom)
- Placement controller (CRUSH-like algorithm)
- Storage engine (redb)
- Erasure encoder/decoder (reed-solomon)
- Gossip protocol for peer discovery
- Self-healing background tasks

### Self-Healing Loops (Automatic, No Operator Intervention)
1. **Heartbeat monitor**: Detect failed nodes via Phi Accrual
2. **Leader election**: Raft automatically elects new leader
3. **Replica repair**: Rebuild missing shards from parity
4. **Rebalancing**: Redistribute data when nodes join/leave
5. **Scrubbing**: Periodic checksum verification

### Why Single Binary Works
- Rust compiles to static binary (no runtime deps)
- All crates linked in (~20-50MB binary)
- Cross-compilation for linux/mac/windows + arm64/amd64
- Same binary for single-node and distributed

### Anti-Patterns We Avoid
| Ceph Style (Complex) | Rucket Style (Simple) |
|---------------------|----------------------|
| Separate mon, osd, mds, rgw daemons | Single `rucket` binary |
| Multiple config files per role | One `rucket.toml` |
| Manual placement group creation | Automatic PG management |
| External etcd/consul for coordination | Built-in Raft |
| Operator required for recovery | Self-healing |

---

## Hidden Problems & Solutions (Critical Review)

### Problem 1: Bootstrap - Who Is First?
**Issue**: Discovery returns empty on first node. How does cluster form?
```bash
# Node 1 starts, queries DNS, gets nothing. Now what?
rucket serve --cluster --discover dns:///rucket.local
```

**Solution**: Bootstrap mode with quorum expectation
```bash
# First node: explicit bootstrap, expect 3 nodes
rucket serve --cluster --bootstrap --expect-nodes 3

# Other nodes: discover and join
rucket serve --cluster --discover dns:///rucket.local
```
Bootstrap node waits for `expect-nodes` before forming cluster. Prevents split-brain on startup.

### Problem 2: Healing Thundering Herd
**Issue**: Node dies, all surviving nodes try to heal same data simultaneously.

**Solution**:
- Healing coordinated by PG leader (only leader initiates healing)
- Global healing rate limit (max N shards/second cluster-wide)
- Backoff when cluster is degraded

### Problem 3: Clock Skew Failure
**Issue**: NTP fails, clocks drift, HLC produces nonsense timestamps.

**Solution**:
- On every RPC, compare local HLC with remote
- If skew > `max_clock_skew` (default: 500ms), reject operation with error
- Emit metric `rucket_clock_skew_seconds`
- Alert if skew approaching threshold

### Problem 4: Strong vs Eventual Operations
**Issue**: "AP with configurable consistency" is too vague.

**Classification**:
| Operation | Consistency | Why |
|-----------|-------------|-----|
| CreateBucket | Strong (Raft) | Must be globally unique |
| DeleteBucket | Strong (Raft) | Must check empty everywhere |
| PutBucketVersioning | Strong (Raft) | Affects all future writes |
| PutBucketPolicy | Strong (Raft) | Security-critical |
| PutObject | Eventual | High throughput needed |
| GetObject | Read-your-writes | User expectation |
| ListObjects | Eventual | Stale listing acceptable |
| DeleteObject | Eventual | Tombstone propagates |

**Read-your-writes**: Client receives `x-rucket-write-hlc` on PUT, sends on subsequent GET. Node waits until local HLC ≥ write HLC before responding.

### Problem 5: LWW Conflict Logging
**Issue**: LWW silently loses data in conflicts.

**Solution**:
- Log all conflicts to `conflict_log` table
- Fields: `bucket`, `key`, `winner_version`, `loser_version`, `winner_hlc`, `loser_hlc`, `timestamp`
- Admin API: `GET /admin/conflicts?bucket=...`
- Metric: `rucket_conflicts_total`
- For versioned buckets: no conflict, both versions kept

### Problem 6: Small Object EC Overhead
**Issue**: EC on 1KB object = 12 shards × overhead > 3x replication

**Solution**:
- `ec_threshold` config (default: 1MB)
- Objects < threshold: 3x replication
- Objects ≥ threshold: erasure coding
- Transparent to client, stored metadata tracks which scheme

### Problem 7: Rolling Upgrade Quorum
**Issue**: Upgrading 3-node cluster can break quorum.

**Solution**: Upgrade procedure (enforced by CLI)
```bash
rucket upgrade start          # Puts cluster in upgrade mode
rucket upgrade drain node1    # Waits for node1 to sync, then stops
# Upgrade node1 binary
rucket upgrade resume node1   # Node1 rejoins, waits for sync
rucket upgrade drain node2    # Repeat
# ...
rucket upgrade finish         # Exits upgrade mode
```
Cluster rejects simultaneous drains that would break quorum.

### Problem 8: Split-Brain Detection
**Issue**: Network partition could create two "primaries".

**Solution**:
- Raft quorum prevents this for metadata operations
- For data writes: require quorum ack (2/3 nodes) before success
- Minority partition: reads work (serve stale), writes fail
- Metric: `rucket_partition_detected` (when can't reach quorum)

### Problem 9: Gossip vs Raft Interaction
**Issue**: Both gossip and Raft do "membership" - overlap unclear.

**Clarification**:
```
Gossip Layer (fast, distributed, eventually consistent)
├── Node discovery: "who exists?"
├── Failure hints: "node X seems down"
└── Metadata dissemination: "cluster config version 42"

Raft Layer (slow, centralized, strongly consistent)
├── Authoritative membership: "these nodes are IN the cluster"
├── Consensus on state changes: "node X is officially dead"
└── Leader election: "node Y is leader for PG Z"
```
Gossip is advisory → feeds into Raft → Raft makes authoritative decisions.

### Problem 10: WAL Corruption Recovery
**Issue**: WAL corrupted on disk, node can't recover.

**Solution**:
- WAL entries have CRC32 checksum
- On corruption detected:
  1. Mark node as "recovering"
  2. Sync from peers (get missing data via replication protocol)
  3. Rebuild local state from peers
  4. Resume normal operation
- If no peers (single node): data loss, but we detect and alert

### Problem 11: Zone-Aware Placement
**Issue**: All replicas could land in same availability zone.

**Solution**:
- Config: `zone = "us-east-1a"` (or auto-detect from cloud metadata)
- Placement rule: replicas must span ≥2 zones (if available)
- CRUSH hierarchy: region → zone → rack → node
- Default: zone-aware if zones detected, otherwise node-aware

### Problem 12: Minimum Required Configuration
**Issue**: We claimed "minimal config" but some things ARE required.

**Actually required**:
```toml
# Minimum viable config
[storage]
data_dir = "/var/lib/rucket"  # REQUIRED (or use default ./data with warning)

[cluster]
discover = "dns:///rucket.local"  # REQUIRED for multi-node
# OR
bootstrap = true
expect_nodes = 3
```

**Sensible defaults for everything else**:
- `bind = "0.0.0.0:9000"`
- `region = "local"`
- `zone = auto-detect or "default"`
- `ec_threshold = "1MB"`
- `max_clock_skew = "500ms"`

---

## Architectural Invariants (Decide NOW, Never Change)

These decisions constrain all future implementation:

### Invariant 1: Object Versions Are Immutable
Once an object version is created, its content NEVER changes.
- Simplifies replication (just ship the version)
- Simplifies caching (version ID = cache key)
- Simplifies conflict resolution (keep all versions)

### Invariant 2: Metadata and Data Are Separated
Metadata (key → location mapping) is separate from data (bytes).
- Metadata can be replicated independently
- Data can use different replication strategy (erasure coding)
- Enables different storage tiers

### Invariant 3: All Operations Are Logged
Every mutation produces a log entry that can be replayed.
- Enables crash recovery
- Enables replication
- Enables point-in-time recovery (future)

### Invariant 4: Placement Is Deterministic
Given (bucket, key, cluster_state), placement is deterministic.
- Any node can compute placement without coordination
- Enables client-side routing (future)
- CRUSH algorithm property

### Invariant 5: Time Is Hybrid Logical
All ordering uses HLC, never wall-clock alone.
- Handles clock skew
- Provides causality
- Enables conflict resolution

---

## What "Forward-Compatible" Actually Means

It's NOT just "add fields now, use later". It's:

### 1. Choosing Abstractions That Don't Leak

**Bad abstraction:**
```rust
fn put_object(data: Bytes) -> Result<()> {
    let file = File::create(path)?;
    file.write_all(&data)?;
    Ok(())
}
```
This LEAKS the assumption of local filesystem. Can't extend to distributed.

**Good abstraction:**
```rust
#[async_trait]
trait StorageBackend {
    async fn put(&self, id: ObjectId, data: Bytes, options: PutOptions) -> Result<PutResult>;
}

struct PutOptions {
    durability: Durability,      // How many replicas before ack?
    placement_hint: Option<PG>,  // Suggest placement
    hlc: HybridClock,           // Logical timestamp
}

struct PutResult {
    version_id: VersionId,
    etag: ETag,
    replicated_to: Vec<NodeId>,  // Which nodes have it?
}
```
This abstraction can be implemented for local, distributed, or geo without changing callers.

### 2. Designing Data Structures for Extension

**Bad structure:**
```rust
struct ObjectMetadata {
    key: String,
    size: u64,
    modified: SystemTime,  // Wall clock - can't compare across nodes
}
```

**Good structure:**
```rust
struct ObjectMetadata {
    key: String,
    size: u64,
    hlc: u64,              // Hybrid logical clock - comparable everywhere

    // Reserved for future, with clear semantics
    #[serde(default)]
    placement: PlacementInfo,
    #[serde(default)]
    replication: ReplicationInfo,
}

#[derive(Default)]
struct PlacementInfo {
    pg: u32,                        // 0 = not sharded yet
    home_region: String,            // "local" = single node
    storage_class: StorageClass,    // Standard = default
}

#[derive(Default)]
struct ReplicationInfo {
    status: ReplicationStatus,
    replicas: Vec<ReplicaInfo>,
}
```

### 3. Building Protocols That Can Evolve

**Bad protocol:**
```
PUT /bucket/key HTTP/1.1
Content-Length: 1234

<data>
```
No versioning, no extension points.

**Good protocol:**
```
PUT /bucket/key HTTP/1.1
Content-Length: 1234
x-rucket-protocol-version: 1
x-rucket-hlc: 1234567890
x-rucket-idempotency-key: abc-123
x-rucket-durability: replicated

<data>
```
Headers provide extension points without breaking existing clients.

---

## Research Foundation

### Foundational Papers

| Concept | Source | Application |
|---------|--------|-------------|
| Hybrid Logical Clocks | [Kulkarni & Demirbas 2014](https://cse.buffalo.edu/~demirbas/publications/hlc.pdf) | Causality tracking in 64 bits |
| CRUSH Algorithm | [Weil et al. SC06](https://ceph.com/assets/pdfs/weil-crush-sc06.pdf) | Decentralized placement |
| Placement Groups | [Ceph RADOS](https://docs.ceph.com/en/reef/architecture/) | Sharding abstraction |
| CRDTs | [Shapiro et al. 2011](https://pages.lip6.fr/Marc.Shapiro/papers/RR-7687.pdf) | Conflict-free replication |
| Locally Repairable Codes | [ACM TOS Survey Dec 2024](https://dl.acm.org/doi/10.1145/3708994) | Erasure coding with locality |
| Raft Consensus | [Ongaro & Ousterhout 2014](https://raft.github.io/raft.pdf) | Metadata consensus |
| Dynamo | Amazon 2007 | Eventually consistent KV |
| Spanner | Google 2012 | Globally consistent DB |

### Latest Research (2024-2025)

#### Erasure Coding Advances
- **Walrus/RedStuff (2025)**: Novel 2D erasure coding achieving 4.5× replication factor with self-healing recovery. First to support storage challenges in async networks. [Paper](https://arxiv.org/abs/2505.05370)
- **TFR-LRCs (Sept 2025)**: Locally Repairable Codes balancing fault tolerance and repair degree. Enables trade-offs: reduce repair degree by slightly increasing storage overhead, or vice versa. [MDPI](https://www.mdpi.com/2078-2489/16/9/803)
- **Wide-Stripe Erasure Coding (2025)**: Solutions for extreme storage savings with wide stripes: Combine Locality (CL) for Azure-LRC, Repair-Contribution-Based Placement (RCBP) for Hitchhiker codes.

#### Raft Consensus Optimizations
- **Fast Raft**: Reduces commit path from 3 rounds to 2 via direct broadcast to designated quorum.
- **C-Raft (Hierarchical)**: Batch local consensus, then order globally—5× throughput improvement for geo deployments.
- **Dynatune (2024)**: Dynamically adapts election timeout/heartbeat based on RTT. Reduces failure detection time by 80%, out-of-service window by 45%.
- **DBSCAN-Raft (2024)**: Election time 19.7ms vs 36.8ms standard Raft for IoT scale.

#### Conflict Resolution & Consistency
- **RedBlue Consistency**: Hybrid model—"blue" ops fast+eventually consistent, "red" ops strongly consistent. Identifies when ops can be blue vs must be red.
- **Eg-walker (EuroSys 2025)**: State-of-the-art CRDT for collaborative editing—better, faster, smaller than Yjs/Automerge.
- **PayPal's Aerospike deployment**: CRDTs with causality tracking for multi-master, multi-datacenter strong eventual consistency.

#### Production Learnings
- **AWS S3 (2025)**: 500 trillion objects, 200M requests/sec, 300+ microservices. ShardStore backend (Rust, append-only LSM). Uses (8+4) erasure coding.
- **CockroachDB**: HLC without specialized hardware (NTP/Amazon Time Sync). Pessimistic write locks + optimistic read refresh = lower latency than Spanner for low-contention.
- **Phi Accrual Failure Detector**: Sweet spot 8 ≤ Φ < 12. Used by Akka, Cassandra. Rust crate exists.

---

## System Architecture Deep Dive

### AWS S3 Architecture
- **Scale**: 500T+ objects, 100M+ requests/sec, 300+ microservices, 11 nines durability
- **ShardStore**: New storage backend written in Rust, LSM-tree with separated keys/values (inspired by WiscKey 2016)
- **Erasure Coding**: (8+4) Reed-Solomon for storage efficiency
- **Consistency**: Strong read-after-write (updated 2020, was eventually consistent)
- **Key Insight**: "1 I/O per second per 2TB" constraint drives placement to spread across millions of drives
- **Source**: [Werner Vogels - All Things Distributed](https://www.allthingsdistributed.com/2023/07/building-and-operating-a-pretty-big-storage-system.html)

### Facebook Storage Evolution
**Haystack (OSDI 2010)**: Photo storage optimized for metadata in memory
- Single disk I/O per read (metadata in RAM)
- Append-only volumes (~100GB each)
- Store + Directory + Cache architecture
- [Paper](https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Beaver.pdf)

**f4 (OSDI 2014)**: Warm blob storage with erasure coding
- Reed-Solomon (10,4) encoding → 1.4x expansion factor
- XOR encoding across 3 datacenters
- Total replication: 2.1x (vs Haystack's 3.6x)
- [Paper](https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-muralidhar.pdf)

**Tectonic (FAST 2021)**: Exabyte-scale unified filesystem
- Layered metadata: Name → File → Block layers (all hash-partitioned)
- Paxos-replicated ZippyDB underneath
- Thick-client architecture for direct disk streaming
- Multi-tenant with per-tenant optimizations
- [Paper](https://www.usenix.org/system/files/fast21-pan.pdf)

### Garage (Deuxfleurs)
- **Goals**: Internet-enabled, self-contained, resilient, simple
- **Explicit Non-Goals**: Extreme performance, feature extensiveness, erasure coding, POSIX
- **Trade-off**: Simplicity over performance; uses only replication (no erasure coding)
- **Philosophy**: "Pursuit of some goals are detrimental to our initial goals"
- [Design Goals](https://garagehq.deuxfleurs.fr/documentation/design/goals/)

### Ceph
- **RADOS**: Reliable Autonomic Distributed Object Store
- **CRUSH Algorithm**: Decentralized placement without central lookup tables
- **Placement Groups**: Objects → PG (via hash) → OSDs (via CRUSH)
- **Cluster Maps**: 5 maps (Monitor, OSD, PG, CRUSH, MDS)
- **Replication**: Primary-copy model, min 2 replicas, recommended 3
- **Erasure Coding**: K+M chunks (e.g., 3+2)
- **Cephx**: Mutual authentication with session keys
- **Scrubbing**: Light (daily metadata) + Deep (weekly bitwise)
- **Papers**: [CRUSH SC06](https://ceph.com/assets/pdfs/weil-crush-sc06.pdf), [RADOS PDSW07](https://ceph.io/assets/pdfs/weil-rados-pdsw07.pdf)

### MinIO
- **Erasure Sets**: 2-16 drives per set, Reed-Solomon encoding
- **Bitrot Detection**: HighwayHash checksums (AVX512 optimized)
- **Healing**: Object-level (not volume-level like RAID)
- **Quorum**: Read=K data shards, Write=K (or K+1 if parity=N/2 to prevent split-brain)
- **2024 Update**: ARM SVE optimizations for HighwayHash
- [Erasure Coding Docs](https://min.io/docs/minio/linux/operations/concepts/erasure-coding.html)

### RustFS
- **Architecture**: Zero-master / decentralized, all nodes equal
- **Storage Model**: Sets (groups of drives), Objects stored in single Set
- **Layers**: API → Distributed Logic → Object (caching, compression, encryption, EC) → Storage
- **Erasure Coding**: Reed-Solomon, tolerates up to parity_drives failures
- **Consistency**: Read-after-write
- **License**: Apache 2.0 (business-friendly, but single-vendor risk)
- [Architecture Docs](https://docs.rustfs.com/concepts/architecture.html)

### SeaweedFS
- **Inspiration**: Facebook Haystack, f4, Tectonic
- **Components**: Master (Raft consensus, 3-5 nodes) → Volume Servers → Filer (optional)
- **Volumes**: 30GB max, ~40 bytes metadata overhead per file
- **Replication**: Topology-aware (same rack: 001, different rack: 010, different DC: 100)
- **Filer Backends**: Redis, Cassandra, MySQL, PostgreSQL, etcd, CockroachDB, etc.
- **Scalability**: O(1) disk seek, billions of files
- [Wiki](https://github.com/seaweedfs/seaweedfs/wiki)

### CockroachDB (SIGMOD 2020)
- **Architecture**: SQL → Transactional → Distribution → Replication → Storage
- **Consensus**: Raft per range (64MB default)
- **MVCC**: HLC timestamps, values never updated in-place
- **Transactions**: Parallel Commits protocol, write intents as provisional values
- **Isolation**: SERIALIZABLE (default) or READ COMMITTED
- **Clock**: HLC without specialized hardware (NTP/Amazon Time Sync)
- [Paper](https://dl.acm.org/doi/pdf/10.1145/3318464.3386134), [Architecture Docs](https://www.cockroachlabs.com/docs/stable/architecture/overview)

### TiDB (VLDB 2020)
- **Components**: TiDB Server (SQL) → TiKV (Storage) → PD (Placement Driver)
- **Regions**: 96MB default, auto-split/merge, 3 replicas per Raft group
- **TiKV**: Rust-based, inspired by Spanner/BigTable/Percolator
- **PD**: "Brain" of cluster—routing table + scheduler + TSO (timestamp oracle)
- **Storage**: RocksDB for data + separate RocksDB for Raft logs per Store
- [Architecture Docs](https://docs.pingcap.com/tidb/stable/tidb-architecture/), [Paper](https://www.vldb.org/pvldb/vol13/p3072-huang.pdf)

---

## Validation Against Latest Research

### ✅ Decision 1: AP with Configurable Consistency — VALIDATED
- **Research confirms**: S3 itself provides strong read-after-write consistency (updated 2020). Our AP default is correct.
- **Enhancement**: Consider RedBlue consistency model for Phase 4—classify ops as "blue" (eventually consistent, fast) vs "red" (strongly consistent).
- **Example**: `ListObjects` can be blue, `PutObject` with conditional headers must be red.

### ✅ Decision 2: Single Raft → Raft-per-PG — VALIDATED
- **Research confirms**: CockroachDB, TiDB use this progression successfully.
- **Enhancement**: Consider Dynatune-style adaptive election timeouts from day 1 (just config, not code).
- **Scale reference**: Single Raft handles ~100M objects; Raft-per-PG scales to billions.

### ✅ Decision 3: Primary-Backup Replication — VALIDATED
- **Research confirms**: CockroachDB uses pessimistic write locks + optimistic read refresh (similar pattern).
- **Alternative considered**: Chain replication (CRAQ) offers strong consistency but higher latency—not S3's model.
- **Keep**: Primary-Backup matches S3 behavior and is simpler.

### ✅ Decision 4: Phi Accrual Failure Detector — VALIDATED
- **Research confirms**: Used by Akka/Cassandra in production. Threshold 8-12 is optimal.
- **Rust implementation**: Crate exists, can use or reference for our implementation.

### ✅ Decision 5: LWW with HLC — VALIDATED, with caveat
- **Research confirms**: CockroachDB, MongoDB, YugabyteDB all use HLC successfully.
- **Caveat**: LWW can lose data in conflict scenarios. For versioned buckets, this is fine (keep all versions). For non-versioned, consider:
  - Keep last N versions internally for conflict auditing
  - Surface conflict metadata in admin API
- **Production validation**: PayPal's Aerospike deployment uses CRDTs with causality for multi-datacenter—our HLC approach is sound.

### ⚠️ Decision 6: Erasure Coding (10+4 RS) — REFINE
- **Original plan**: Reed-Solomon (10+4), upgrade to LRC.
- **Research update**: Walrus's RedStuff achieves 4.5× replication with 2D erasure coding and self-healing.
- **Recommendation**:
  - Phase 3: Start with (8+4) RS (matches AWS S3)
  - Phase 3.5: Evaluate LRC vs RedStuff based on:
    - Repair bandwidth requirements
    - Cross-rack topology
    - Async network tolerance

### ⚠️ Decision 7: CRUSH for Placement — REFINE
- **Original plan**: CRUSH algorithm (from Ceph).
- **Research update**: MapX extends CRUSH with time-dimension mapping for controlled migration.
- **Recommendation**: Standard CRUSH is sufficient for Phase 3. Consider MapX for cluster expansion scenarios in Phase 3.5.

---

## Core Architecture Principles

### 1. Location-Aware Data Model (Implement in Phase 1)

Every object has placement metadata from day 1, even if unused:

```rust
struct ObjectMetadata {
    // Existing fields
    key: String,
    version_id: String,
    etag: String,
    size: u64,
    content_type: String,

    // Forward-compatible fields (add NOW)
    hlc_timestamp: u64,        // Hybrid Logical Clock (64-bit)
    placement_group: u32,      // PG for sharding (default: 0)
    home_region: String,       // Region ID (default: "local")
    replication_status: Option<ReplicationStatus>,
    storage_class: StorageClass,  // For tiering
}

enum ReplicationStatus {
    None,
    Pending { target_regions: Vec<String> },
    Replicated { regions: Vec<String>, hlc: u64 },
    Failed { reason: String },
}

enum StorageClass {
    Standard,      // Hot data
    InfrequentAccess,
    Archive,       // Cold data (future tiering)
}
```

**Why now?** These fields cost almost nothing when unused but prevent schema migrations later.

### 2. Hybrid Logical Clocks (Implement in Phase 1)

Use HLC instead of system timestamps for all operations:

```rust
struct HybridLogicalClock {
    // Fits in 64 bits: 48 bits physical + 16 bits logical
    physical_ms: u48,  // Milliseconds since epoch
    logical: u16,      // Counter for same-millisecond events
}

impl HybridLogicalClock {
    fn now(&mut self) -> u64 {
        let pt = system_time_ms();
        if pt > self.physical_ms {
            self.physical_ms = pt;
            self.logical = 0;
        } else {
            self.logical += 1;
        }
        self.pack()
    }

    fn receive(&mut self, remote: u64) -> u64 {
        let (remote_pt, remote_l) = unpack(remote);
        let pt = system_time_ms();

        if pt > self.physical_ms && pt > remote_pt {
            self.physical_ms = pt;
            self.logical = 0;
        } else if self.physical_ms > remote_pt {
            self.logical += 1;
        } else if remote_pt > self.physical_ms {
            self.physical_ms = remote_pt;
            self.logical = remote_l + 1;
        } else {
            self.logical = max(self.logical, remote_l) + 1;
        }
        self.pack()
    }
}
```

**Why HLC over timestamps?**
- Captures causality (A → B means HLC(A) < HLC(B))
- Constant space (unlike vector clocks which grow with nodes)
- Monotonic (never goes backwards)
- Close to real time (within clock skew)
- Used by CockroachDB, MongoDB, YugabyteDB

### 3. Placement Group Abstraction (Implement in Phase 1)

Abstract all object operations through placement groups:

```rust
trait PlacementPolicy {
    fn get_pg(&self, bucket: &str, key: &str) -> PlacementGroup;
    fn get_nodes(&self, pg: &PlacementGroup) -> Vec<NodeId>;
}

// Phase 1: Single node, single PG
struct SingleNodePlacement;
impl PlacementPolicy for SingleNodePlacement {
    fn get_pg(&self, _bucket: &str, _key: &str) -> PlacementGroup {
        PlacementGroup(0)  // Everything goes to PG 0
    }
    fn get_nodes(&self, _pg: &PlacementGroup) -> Vec<NodeId> {
        vec![NodeId::Local]  // Only local node
    }
}

// Phase 3: CRUSH-like placement
struct CrushPlacement {
    crush_map: CrushMap,
    pg_count: u32,
}
impl PlacementPolicy for CrushPlacement {
    fn get_pg(&self, bucket: &str, key: &str) -> PlacementGroup {
        // Consistent hash to PG
        let hash = xxhash(format!("{}/{}", bucket, key));
        PlacementGroup(hash % self.pg_count)
    }
    fn get_nodes(&self, pg: &PlacementGroup) -> Vec<NodeId> {
        self.crush_map.select(pg, /*replica_count=*/3)
    }
}

// Phase 4: Region-aware placement
struct GeoPlacement {
    local_region: String,
    regions: HashMap<String, CrushPlacement>,
    replication_policy: ReplicationPolicy,
}
```

**Why now?** The abstraction costs nothing but enables seamless transition.

### 4. Event-Sourced Write Path (Implement in Phase 1)

All mutations flow through an event log:

```rust
enum StorageEvent {
    ObjectPut {
        bucket: String,
        key: String,
        version_id: String,
        hlc: u64,
        size: u64,
        etag: String,
        storage_path: PathBuf,
    },
    ObjectDelete {
        bucket: String,
        key: String,
        version_id: String,
        hlc: u64,
        is_delete_marker: bool,
    },
    BucketCreate { name: String, hlc: u64 },
    BucketDelete { name: String, hlc: u64 },
    // ... more events
}

trait EventLog {
    fn append(&self, event: StorageEvent) -> Result<u64>;  // Returns LSN
    fn read_from(&self, lsn: u64) -> impl Iterator<Item = StorageEvent>;
    fn checkpoint(&self, lsn: u64) -> Result<()>;
}
```

**Why now?**
- WAL already exists, just needs structured events
- Enables replay for crash recovery (Phase 1)
- Enables replication by shipping events (Phase 3-4)
- Enables CDC/change notifications (future)

### 5. Storage Backend Abstraction (Implement in Phase 1)

Abstract storage to support future backends:

```rust
#[async_trait]
trait StorageBackend: Send + Sync {
    async fn put(&self, path: &StoragePath, data: Bytes) -> Result<()>;
    async fn get(&self, path: &StoragePath) -> Result<Bytes>;
    async fn delete(&self, path: &StoragePath) -> Result<()>;
    async fn exists(&self, path: &StoragePath) -> Result<bool>;

    // Erasure coding support (Phase 3)
    fn supports_erasure_coding(&self) -> bool { false }
    async fn put_coded(&self, _path: &StoragePath, _shards: Vec<Bytes>) -> Result<()> {
        unimplemented!()
    }
}

// Phase 1: Local filesystem
struct LocalBackend { data_dir: PathBuf }

// Phase 3: Distributed backend
struct DistributedBackend {
    local: LocalBackend,
    placement: Box<dyn PlacementPolicy>,
    transport: Box<dyn NodeTransport>,
}

// Phase 3: Erasure coded backend
struct ErasureCodingBackend {
    inner: Box<dyn StorageBackend>,
    encoder: ReedSolomonEncoder,  // e.g., 10+4 LRC
}
```

---

## Phase-by-Phase Implementation

### Phase 1: S3 Parity (Now)

**What to implement:**
- [x] Add `hlc_timestamp` to all object metadata (use current time initially)
- [x] Add `placement_group: u32` field (always 0)
- [x] Add `home_region: String` field (always "local")
- [x] Add `storage_class` field (always Standard)
- [x] Wrap WAL events in `StorageEvent` enum
- [x] Add `PlacementPolicy` trait with `SingleNodePlacement`

**What NOT to do:**
- Don't implement multi-node transport yet
- Don't implement erasure coding yet
- Don't implement replication yet

**Files to modify:**
- `crates/rucket-core/src/types.rs` - Add new fields
- `crates/rucket-storage/src/wal/` - Structured events
- `crates/rucket-storage/src/lib.rs` - PlacementPolicy trait

### Phase 2: Production

**What to implement:**
- Object Lock (compliance, governance modes)
- SSE-S3 encryption
- Hardened WAL recovery

**Forward-compatible additions:**
- Add `lock_config: Option<ObjectLockConfig>` to object metadata
- Add `encryption_config: Option<EncryptionConfig>` to bucket metadata

### Phase 3: Distributed

**What to implement:**
- Multi-node clustering with Raft for metadata consensus
- CRUSH-like placement algorithm
- Erasure coding (start with RS, then LRC)
- PG-based sharding

**Architecture:**

```
┌──────────────────────────────────────────────────────────────────────┐
│                           Client Request                             │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  v
┌──────────────────────────────────────────────────────────────────────┐
│                          API Gateway Node                            │
│                                                                      │
│   ┌────────────────┐   ┌────────────────┐   ┌────────────────┐       │
│   │   Placement    │   │   Route to     │   │   Aggregate    │       │
│   │     Policy     │-->│   PG Owner     │-->│    Response    │       │
│   └────────────────┘   └────────────────┘   └────────────────┘       │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
                                  │
             ┌────────────────────┼────────────────────┐
             v                    v                    v
  ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
  │     Node 1     │   │     Node 2     │   │     Node 3     │
  │   PG 0, 3, 6   │   │   PG 1, 4, 7   │   │   PG 2, 5, 8   │
  │  ┌──────────┐  │   │  ┌──────────┐  │   │  ┌──────────┐  │
  │  │   Raft   │<-┼---┼->│   Raft   │<-┼---┼->│   Raft   │  │
  │  │  Leader  │  │   │  │ Follower │  │   │  │ Follower │  │
  │  └──────────┘  │   │  └──────────┘  │   │  └──────────┘  │
  └────────────────┘   └────────────────┘   └────────────────┘
```

**Metadata Consensus (Raft per PG):**
- Each PG has a Raft group (1 leader + N followers)
- Metadata operations go through Raft for linearizability
- Data writes go directly to storage nodes (quorum)

**Erasure Coding:**
- Start with Reed-Solomon (10+4)
- Upgrade to LRC for better repair locality
- Storage path: `{pg}/{object_id}/{shard_id}.dat`

### Phase 4: Geo-Distribution

**What to implement:**
- Cross-region async replication
- Conflict resolution with HLC
- S3 CRR API compatibility

**Architecture:**

```
┌─────────────────────────────────────────────────────────┐
│                      Region: us-east                     │
│  ┌──────────────────────────────────────────────────┐   │
│  │              Cluster (Phase 3)                    │   │
│  └──────────────────────────────────────────────────┘   │
│                          │                               │
│              ┌───────────▼───────────┐                  │
│              │   Replication Log     │                  │
│              │   (Event Stream)      │                  │
│              └───────────┬───────────┘                  │
└──────────────────────────┼──────────────────────────────┘
                           │ Async replication
                           ▼
┌──────────────────────────┼──────────────────────────────┐
│                      Region: eu-west                     │
│              ┌───────────▼───────────┐                  │
│              │   Replication Log     │                  │
│              │   (Event Consumer)    │                  │
│              └───────────┬───────────┘                  │
│                          │                               │
│  ┌──────────────────────────────────────────────────┐   │
│  │              Cluster (Phase 3)                    │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

**Conflict Resolution:**
- Last-Writer-Wins using HLC (already in place from Phase 1)
- Concurrent writes (same HLC physical time) resolved by:
  1. Higher logical counter wins
  2. If equal, lexicographic region ID wins (deterministic)

**Replication Protocol:**
```rust
struct ReplicationEvent {
    event: StorageEvent,
    source_region: String,
    hlc: u64,
    vector_clock: HashMap<String, u64>,  // Per-region HLC
}

// Merge function (CRDT-style)
fn merge(local: &ObjectMetadata, remote: &ReplicationEvent) -> ObjectMetadata {
    if remote.hlc > local.hlc_timestamp {
        // Remote wins
        apply(remote)
    } else if remote.hlc == local.hlc_timestamp {
        // Tie-breaker: lexicographic region ID
        if remote.source_region > local.home_region {
            apply(remote)
        }
        // else keep local
    }
    // else keep local (local is newer)
}
```

---

## What to Implement NOW (Phase 1)

To avoid retrofitting, add these fields/abstractions immediately:

### 1. Object Metadata Changes

```rust
// In crates/rucket-core/src/types.rs
pub struct ObjectMetadata {
    // ... existing fields ...

    // NEW: Forward-compatible fields
    pub hlc_timestamp: u64,           // Default: current_time_ms << 16
    pub placement_group: u32,         // Default: 0
    pub home_region: String,          // Default: "local"
    pub replication_status: Option<ReplicationStatus>,
    pub storage_class: StorageClass,
}
```

### 2. Placement Abstraction

```rust
// In crates/rucket-storage/src/placement.rs (new file)
pub trait PlacementPolicy: Send + Sync {
    fn get_pg(&self, bucket: &str, key: &str) -> u32;
}

pub struct SingleNodePlacement;
impl PlacementPolicy for SingleNodePlacement {
    fn get_pg(&self, _: &str, _: &str) -> u32 { 0 }
}
```

### 3. Structured Events

```rust
// In crates/rucket-storage/src/events.rs (new file)
#[derive(Serialize, Deserialize)]
pub enum StorageEvent {
    ObjectPut { bucket: String, key: String, version_id: String, hlc: u64, ... },
    ObjectDelete { bucket: String, key: String, version_id: String, hlc: u64, ... },
    // ...
}
```

### 4. HLC Module

```rust
// In crates/rucket-core/src/hlc.rs (new file)
pub struct HybridLogicalClock { ... }
impl HybridLogicalClock {
    pub fn now(&mut self) -> u64;
    pub fn receive(&mut self, remote: u64) -> u64;
}
```

---

## Summary: No Retrofitting Design

| Feature | Implemented In | Prepared For |
|---------|----------------|--------------|
| HLC timestamps | Phase 1 | Phase 3-4 conflict resolution |
| Placement groups | Phase 1 (always 0) | Phase 3 sharding |
| Home region | Phase 1 ("local") | Phase 4 geo-replication |
| Storage class | Phase 1 (Standard) | Phase 3 tiering |
| Event log | Phase 1 (WAL) | Phase 3-4 replication |
| Placement abstraction | Phase 1 | Phase 3-4 |

**Key insight:** The cost of adding unused fields is negligible. The cost of retrofitting is enormous.

---

# Rucket Vision

> **Status:** Internal planning document
> **Last Updated:** 2026-01-03

---

## Why Rucket Exists

**Origin:** MinIO frustration + OSS philosophy + learning Rust storage systems.

The S3-compatible storage landscape is broken:
- **MinIO:** AGPL but single-vendor controlled, actively hostile to community, stripping features for paid version
- **RustFS:** Apache 2.0 + single vendor = same rugpull risk as early MinIO
- **Garage:** AGPL + non-profit, but explicitly rejects features (versioning, tagging, performance)
- **CubeFS:** CNCF graduated, but multi-protocol (POSIX/HDFS/S3) = complexity overkill. S3 is bolted-on, not native. Apache 2.0 = no copyleft.
- **Ceph:** Foundation-governed, full S3, but massive complexity - designed for petabyte-scale, not self-hosters
- **SeaweedFS:** Apache 2.0 single-vendor, S3 compatibility less mature than MinIO

**Rucket fills the gap:** A truly community-governed, feature-complete, high-performance S3 implementation.

---

## Goals

### G1: True Open Source
- **License:** AGPL v3 (copyleft, no proprietary forks)
- **Governance:** Foundation model (eventual non-profit, no single-vendor control)
- **Codebase:** Simple enough to audit, understand, and fork

### G2: S3 Feature Completeness
Drop-in replacement for S3, not "S3-ish". Target: 90%+ Ceph s3-tests compatibility.

| Feature | Status | Garage |
|---------|--------|--------|
| Object versioning | ✅ | ❌ never |
| Object tagging | ✅ | ❌ never |
| Bucket tagging | ✅ | ❌ never |
| Checksums | ✅ | ❌ |
| Multipart uploads | ✅ | ✅ |
| Presigned URLs | ✅ | ✅ |
| Bucket policies (enforced) | 🔲 | ❌ never |
| Object Lock | 🔲 | ❌ never |
| SSE-S3/SSE-KMS | 🔲 | ❌ |

### G3: Performance
Design for performance from the ground up, not bolted on later.
- Competitive with MinIO on single-node workloads
- Sub-millisecond metadata operations
- Efficient streaming for large objects
- Benchmark-driven development

### G4: Stability & Durability
Data safety is non-negotiable.
- Write-ahead log with configurable durability
- Crash recovery that actually works
- No data loss under any failure mode
- Designed for production from day one

### G5: Distributed (Future)
Single-node first, but designed for distributed from the start. No architectural debt.

**Design principle:** Architecture supports geo from day 1, but `rucket serve` remains the simple entry point. Complexity scales with need, not with capability.

**Progressive complexity:**
```
rucket serve                    # Single node - just works
rucket serve --cluster          # Single-site cluster
rucket serve --cluster --geo    # Multi-region replication
```

**Scalability:**
- Erasure coding for storage efficiency (don't require 3x replication)
- Efficient handling of billions of objects
- Storage tiering (hot/cold data)

**High Availability:**
- No single point of failure
- Automatic failover
- Data redundancy across nodes

**Clustering:**
- Horizontal scaling across nodes
- Consistent hashing / data placement
- Architecture TBD (consensus, replication strategy)

**Geo-Distribution:**
- Cross-region replication (async by default, sync optional)
- Region-aware object placement
- Disaster recovery across sites
- Designed into core from day 1, not bolted on later

---

## Non-Goals

- **Multi-protocol:** No POSIX, HDFS, NFS. S3 only. (Use CubeFS if you need multi-protocol)
- **Enterprise sales model:** No paid features, no "community vs enterprise" split
- **Kubernetes-only:** Should run anywhere - bare metal, Docker, k8s, embedded

---

## Target Users

1. **Developers:** Local dev/testing, CI pipelines, mock S3 for tests
2. **Self-hosters:** Homelab, personal cloud, small organizations
3. **SMB Production:** Small-medium businesses needing reliable S3 storage

General-purpose. No artificial limitations.

---

## Success Metrics

| Metric | Target |
|--------|--------|
| S3 Compatibility | 90%+ Ceph s3-tests |
| Performance | Within 20% of MinIO on benchmarks |
| Community | First external contributor |
| Production | First external production deployment |

---

## Competitive Position

| | License | Governance | S3 Features | Distributed | Simple | Notes |
|---|---------|------------|-------------|-------------|--------|-------|
| MinIO | AGPL | Single vendor ❌ | Full | ✅ | ✅ | Feature stripping for paid version |
| RustFS | Apache | Single vendor ❌ | Full | ✅ | ✅ | Same rugpull risk as early MinIO |
| Garage | AGPL | Non-profit ✅ | Minimal ❌ | ✅ | ✅ | Explicitly rejects versioning/tagging |
| CubeFS | Apache | CNCF ✅ | Partial | ✅ | ❌ | Multi-protocol focus, complex |
| Ceph | LGPL | Foundation ✅ | Full | ✅ | ❌ | Overkill for most use cases |
| SeaweedFS | Apache | Single vendor ❌ | Full | ✅ | ⚠️ | Less mature S3 compatibility |
| **Rucket** | **AGPL** | **Foundation ✅** | **Full** | **🔲** | **✅** | Simple by default, scales to global |

**Why not CubeFS?** CNCF graduated (Jan 2025), but:
- Multi-protocol (POSIX, HDFS, S3) = complexity for S3-only users
- Apache 2.0 = no copyleft protection
- S3 is add-on, not core - versioning/tagging support unclear
- Designed for large-scale deployments, not self-hosters

---

## Roadmap

### Phase 1: S3 Parity (Current)
- Fix compatibility bugs (SigV4, versioning, pagination)
- Pass 80%+ Ceph s3-tests
- Enforce bucket policies
- Performance baseline benchmarks

### Phase 2: Production-Ready
- Object Lock (compliance/governance modes)
- SSE-S3 encryption at rest
- Hardened crash recovery
- Documentation for production deployment

### Phase 3: Distributed Foundation
- Research distributed architectures (consensus, replication, placement)
- **Design geo-aware data model from day 1** (region, replication_status, version vectors)
- Erasure coding implementation
- Storage tiering (hot/cold)
- Optimized metadata for billions of objects
- HA: active-passive failover as first step
- Clustering: multi-node horizontal scaling

### Phase 4: Geo-Distribution
- Cross-region async replication
- Region-aware placement policies
- Conflict resolution (last-writer-wins with vector clocks)
- Sync replication option for low-latency regions
- S3 Cross-Region Replication (CRR) API compatibility

### Phase 5: Community & Governance
- Governance documentation (decision-making process)
- Contributor guide
- Foundation setup (if adoption warrants)
- First external maintainer

---

## Principles

1. **No compromises on quality.** Don't ship broken things. Don't bolt on features later.
2. **Compatibility is the product.** If AWS SDK doesn't work, it's a bug.
3. **Simple > Clever.** Readable code over clever optimizations.
4. **Community > Company.** Decisions benefit users, not a business model.
5. **Complexity scales with need.** `rucket serve` just works. Advanced features are opt-in, not forced.

---

*This document defines what Rucket is. All technical decisions should align with these goals.*
