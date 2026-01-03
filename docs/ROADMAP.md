# Rucket Master Roadmap: From Current State to Distributed S3

## Executive Summary

**Current State**: ~85-90% Phase 1 complete (single-node S3 API)
**Target State**: Phase 5 (geo-distributed, community-governed)
**Critical Gap**: No forward-compatible data model for distributed features

---

## Current State Assessment

### What Exists Today

| Component | Status | Location |
|-----------|--------|----------|
| S3 API (Buckets, Objects, Multipart) | Complete | `crates/rucket-api/src/handlers/` |
| Object Versioning | Complete | `handlers/object.rs`, `types.rs` |
| Object Tagging, CORS | Complete | `handlers/object.rs`, `handlers/bucket.rs` |
| SigV4 Authentication | Complete | `auth/sigv4.rs` |
| WAL with crash recovery | Complete | `crates/rucket-storage/src/wal/` |
| redb metadata storage | Complete | `metadata/redb_backend.rs` |
| Configurable sync strategies | Complete | `sync.rs` |
| Prometheus metrics | Complete | `metrics.rs` |
| CI/CD pipeline | Complete | `.github/workflows/ci.yml` |
| S3 compatibility tests | Complete | `scripts/s3-compat-tests.sh` |

### What's Missing (per architecture.md)

| Component | Phase Required | Status |
|-----------|----------------|--------|
| HLC timestamps on objects | Phase 1 (prep) | **NOT STARTED** |
| placement_group field | Phase 1 (prep) | **NOT STARTED** |
| home_region field | Phase 1 (prep) | **NOT STARTED** |
| storage_class field | Phase 1 (prep) | **NOT STARTED** |
| PlacementPolicy trait | Phase 1 (prep) | **NOT STARTED** |
| StorageEvent wrapper | Phase 1 (prep) | **NOT STARTED** |
| Object Lock | Phase 2 | **NOT STARTED** |
| SSE-S3 Encryption | Phase 2 | **NOT STARTED** |
| Bucket Policies | Phase 2 | **NOT STARTED** |
| Raft consensus | Phase 3 | **NOT STARTED** |
| CRUSH placement | Phase 3 | **NOT STARTED** |
| Erasure coding | Phase 3 | **NOT STARTED** |
| Geo-replication | Phase 4 | **NOT STARTED** |

---

## Phase 1: Foundation Completion

**Goal**: Complete S3 parity + add forward-compatible data model

### Milestone 1.1: Forward-Compatible Data Model

**Deliverable**: Object metadata ready for distributed future

**Files to modify**:
- `crates/rucket-core/src/types.rs`
- `crates/rucket-core/src/lib.rs`
- `crates/rucket-storage/src/metadata/redb_backend.rs`

**Tasks**:
1. [ ] Create `crates/rucket-core/src/hlc.rs` - Hybrid Logical Clock implementation
   - `HlcTimestamp` struct (physical_time: u64, logical_counter: u16)
   - `HlcClock` with `now()`, `update(remote_hlc)`, `compare()`
   - Unit tests for clock ordering and skew detection

2. [ ] Add fields to `ObjectMetadata` in `types.rs`:
   ```rust
   pub hlc_timestamp: u64,           // Default: 0 (Phase 1)
   pub placement_group: u32,         // Default: 0 (single node)
   pub home_region: String,          // Default: "local"
   pub storage_class: StorageClass,  // Default: Standard
   pub replication_status: Option<ReplicationStatus>,
   ```

3. [ ] Create `StorageClass` enum:
   ```rust
   enum StorageClass { Standard, InfrequentAccess, Archive }
   ```

4. [ ] Create `ReplicationStatus` enum:
   ```rust
   enum ReplicationStatus {
       None,
       Pending { target_regions: Vec<String> },
       Replicated { regions: Vec<String>, hlc: u64 },
       Failed { reason: String },
   }
   ```

5. [ ] Add fields to `BucketInfo` in `types.rs`:
   ```rust
   pub encryption_config: Option<EncryptionConfig>,
   pub lock_config: Option<ObjectLockConfig>,
   ```

6. [ ] Update redb schema for new fields (backward compatible with #[serde(default)])

7. [ ] Add migration test: old metadata still deserializes

**Testing**:
- [ ] Unit tests for HLC ordering
- [ ] Unit tests for new metadata serialization/deserialization
- [ ] Integration test: create objects, verify new fields present
- [ ] Regression test: old persisted data still loads

**CI adjustment**: None required (existing tests cover)

---

### Milestone 1.2: PlacementPolicy Abstraction

**Deliverable**: Trait abstraction enabling future sharding

**Files to modify**:
- `crates/rucket-storage/src/placement.rs` (new)
- `crates/rucket-storage/src/lib.rs`
- `crates/rucket-storage/src/local.rs`

**Tasks**:
1. [ ] Create `crates/rucket-storage/src/placement.rs`:
   ```rust
   pub trait PlacementPolicy: Send + Sync {
       fn compute_placement(&self, bucket: &str, key: &str) -> PlacementResult;
       fn get_nodes_for_pg(&self, pg: u32) -> Vec<NodeId>;
   }

   pub struct PlacementResult {
       pub placement_group: u32,
       pub primary_node: NodeId,
       pub replica_nodes: Vec<NodeId>,
   }

   pub struct SingleNodePlacement;
   impl PlacementPolicy for SingleNodePlacement {
       // Always returns PG=0, local node
   }
   ```

2. [ ] Wire PlacementPolicy into LocalStorage (use SingleNodePlacement)

3. [ ] Add placement_group to WAL entries

**Testing**:
- [ ] Unit test: SingleNodePlacement always returns PG=0
- [ ] Integration test: objects written with placement_group=0

**CI adjustment**: None required

---

### Milestone 1.3: StorageEvent Wrapper

**Deliverable**: High-level events for future replication streaming

**Files to modify**:
- `crates/rucket-storage/src/events.rs` (new)
- `crates/rucket-storage/src/wal/entry.rs`
- `crates/rucket-storage/src/local.rs`

**Tasks**:
1. [ ] Create `crates/rucket-storage/src/events.rs`:
   ```rust
   pub enum StorageEvent {
       ObjectCreated {
           bucket: String,
           key: String,
           version_id: String,
           hlc: u64,
           size: u64,
           etag: String,
       },
       ObjectDeleted {
           bucket: String,
           key: String,
           version_id: Option<String>,
           hlc: u64,
           is_delete_marker: bool,
       },
       BucketCreated { bucket: String, hlc: u64 },
       BucketDeleted { bucket: String, hlc: u64 },
   }
   ```

2. [ ] Emit StorageEvent after each WAL commit

3. [ ] Add optional event callback to LocalStorage config

**Testing**:
- [ ] Unit test: events emitted for all operations
- [ ] Integration test: event stream matches operations

**CI adjustment**: None required

---

### Milestone 1.4: S3 Compatibility Polish

**Deliverable**: Pass 90%+ Ceph s3-tests

**Files to modify**:
- Various handlers in `crates/rucket-api/src/handlers/`

**Tasks**:
1. [ ] Run full Ceph s3-tests, capture baseline pass rate
2. [ ] Triage failures into: critical, nice-to-have, won't-fix
3. [ ] Fix critical S3 compatibility issues (target: 90%+)
4. [ ] Document known incompatibilities

**Testing**:
- [ ] Full Ceph s3-tests run
- [ ] Document pass rate in README

**CI adjustment**:
- [ ] Add s3-compat job to CI (optional, can fail for now)

---

### Milestone 1.5: Performance Baseline

**Deliverable**: Documented performance metrics for comparison

**Tasks**:
1. [ ] Run throughput benchmarks (PUT/GET varying sizes)
2. [ ] Compare against MinIO on same hardware
3. [ ] Document baseline in `docs/benchmarks/`
4. [ ] Establish regression threshold (within 20% of MinIO)

**Testing**:
- [ ] Automated benchmark script
- [ ] Results stored for comparison

**CI adjustment**:
- [ ] Add benchmark job (weekly, not blocking)

---

## Phase 2: Production-Ready

**Goal**: Security features + hardened durability

### Milestone 2.1: Object Lock (Compliance + Governance)

**Deliverable**: S3 Object Lock API compatibility

**Files to modify**:
- `crates/rucket-core/src/types.rs`
- `crates/rucket-api/src/handlers/object.rs`
- `crates/rucket-api/src/handlers/bucket.rs`
- `crates/rucket-storage/src/metadata/redb_backend.rs`

**Tasks**:
1. [ ] Define ObjectLockConfig, RetentionMode, LegalHold types
2. [ ] Implement bucket-level Object Lock configuration
3. [ ] Implement object-level retention/legal hold
4. [ ] Enforce retention during delete operations
5. [ ] Implement GetObjectLockConfiguration, PutObjectLockConfiguration
6. [ ] Implement GetObjectRetention, PutObjectRetention
7. [ ] Implement GetObjectLegalHold, PutObjectLegalHold

**Testing**:
- [ ] Unit tests for retention logic
- [ ] Integration tests for lock enforcement
- [ ] S3 compatibility tests for Object Lock APIs

**CI adjustment**: None required

---

### Milestone 2.2: SSE-S3 Encryption at Rest

**Deliverable**: Server-side encryption with managed keys

**Files to modify**:
- `crates/rucket-core/src/crypto.rs` (new)
- `crates/rucket-storage/src/local.rs`
- `crates/rucket-api/src/handlers/object.rs`

**Tasks**:
1. [ ] Add `ring` or `aes-gcm` dependency for encryption
2. [ ] Implement key derivation from master secret
3. [ ] Encrypt object data on write
4. [ ] Decrypt object data on read
5. [ ] Store encryption metadata
6. [ ] Support x-amz-server-side-encryption header
7. [ ] Implement bucket default encryption

**Testing**:
- [ ] Unit tests for encrypt/decrypt round-trip
- [ ] Integration tests: encrypted objects readable
- [ ] Verify encryption at rest (raw file inspection)

**CI adjustment**: None required

---

### Milestone 2.3: Bucket Policies

**Deliverable**: IAM-style bucket policies

**Files to modify**:
- `crates/rucket-core/src/policy.rs` (new)
- `crates/rucket-api/src/handlers/bucket.rs`
- `crates/rucket-api/src/auth/mod.rs`

**Tasks**:
1. [ ] Define Policy JSON schema (Principal, Action, Resource, Condition)
2. [ ] Implement policy parsing and validation
3. [ ] Implement policy evaluation engine
4. [ ] Integrate policy checks into request flow
5. [ ] Implement GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy

**Testing**:
- [ ] Unit tests for policy evaluation
- [ ] Integration tests for access control
- [ ] Ceph s3-tests for bucket policy APIs

**CI adjustment**: None required

---

### Milestone 2.4: Hardened WAL Recovery

**Deliverable**: Bulletproof crash recovery

**Files to modify**:
- `crates/rucket-storage/src/wal/recovery.rs`
- `crates/rucket-storage/src/wal/writer.rs`

**Tasks**:
1. [ ] Add CRC32 checksums to WAL entries
2. [ ] Implement WAL segment rotation
3. [ ] Implement checksum verification on recovery
4. [ ] Add corruption detection and handling
5. [ ] Implement sync-from-peer option (Phase 3 prep)
6. [ ] Fuzz testing for corruption scenarios

**Testing**:
- [ ] Chaos tests: kill during write, verify recovery
- [ ] Corruption injection tests
- [ ] Multi-scenario recovery tests

**CI adjustment**:
- [ ] Add fuzz testing job (optional)

---

### Milestone 2.5: Production Documentation

**Deliverable**: Deployment-ready documentation

**Tasks**:
1. [ ] Write deployment guide (Docker, systemd, K8s)
2. [ ] Write operations guide (backup, restore, monitoring)
3. [ ] Write security hardening guide
4. [ ] Write troubleshooting guide
5. [ ] Performance tuning guide

**CI adjustment**: None required

---

## Phase 3: Distributed Foundation

**Goal**: Multi-node clustering with HA

### Milestone 3.1: Raft Consensus (Metadata Only)

**Deliverable**: Consistent metadata across nodes

**New dependencies**:
- `openraft` (or custom implementation)

**Files to modify**:
- `crates/rucket-consensus/` (new crate)
- `crates/rucket/src/main.rs`

**Tasks**:
1. [ ] Add openraft dependency
2. [ ] Implement RaftStorage trait for redb
3. [ ] Implement Raft state machine for metadata operations
4. [ ] Add cluster configuration (peers, discovery)
5. [ ] Implement leader election
6. [ ] Wire Raft into bucket/metadata operations
7. [ ] Implement cluster bootstrap mode

**Testing**:
- [ ] Unit tests for Raft state machine
- [ ] 3-node cluster tests
- [ ] Leader failover tests
- [ ] Split-brain prevention tests

**CI adjustment**:
- [ ] Add multi-node test job
- [ ] Add cluster integration tests

---

### Milestone 3.2: Placement Groups + CRUSH

**Deliverable**: Deterministic data placement

**New dependencies**:
- `crush` (or custom implementation)

**Files to modify**:
- `crates/rucket-placement/` (new crate)
- `crates/rucket-storage/src/placement.rs`

**Tasks**:
1. [ ] Implement CRUSH algorithm (or port existing)
2. [ ] Implement CRUSHPlacement policy
3. [ ] Implement cluster map (nodes, weights, topology)
4. [ ] Implement zone-aware placement
5. [ ] Wire CRUSH into storage routing
6. [ ] Implement PG ownership calculation

**Testing**:
- [ ] Unit tests for CRUSH placement consistency
- [ ] Distribution uniformity tests
- [ ] Zone awareness tests

**CI adjustment**: None required

---

### Milestone 3.3: Erasure Coding (8+4 RS)

**Deliverable**: Storage-efficient durability

**New dependencies**:
- `reed-solomon-erasure`

**Files to modify**:
- `crates/rucket-erasure/` (new crate)
- `crates/rucket-storage/src/backend.rs`

**Tasks**:
1. [ ] Add reed-solomon-erasure dependency
2. [ ] Implement ErasureBackend trait
3. [ ] Implement (8+4) RS encoder/decoder
4. [ ] Implement shard distribution to nodes
5. [ ] Implement shard retrieval and reconstruction
6. [ ] Add ec_threshold config (default 1MB)
7. [ ] Objects < threshold use replication

**Testing**:
- [ ] Unit tests for encode/decode
- [ ] Failure simulation (lose shards, recover)
- [ ] Performance benchmarks

**CI adjustment**: None required

---

### Milestone 3.4: Primary-Backup Replication

**Deliverable**: Configurable replication levels

**Files to modify**:
- `crates/rucket-replication/` (new crate)
- `crates/rucket-storage/src/local.rs`

**Tasks**:
1. [ ] Implement replication levels: local, replicated, durable
2. [ ] Implement async replication to backups
3. [ ] Implement sync replication with quorum ack
4. [ ] Implement replication lag monitoring
5. [ ] Wire replication into write path

**Testing**:
- [ ] Unit tests for replication logic
- [ ] Integration tests for async/sync modes
- [ ] Failure scenarios (backup down)

**CI adjustment**: None required

---

### Milestone 3.5: Failure Detection + Self-Healing

**Deliverable**: Automatic recovery from failures

**Files to modify**:
- `crates/rucket-cluster/` (new crate)

**Tasks**:
1. [ ] Implement Phi Accrual Failure Detector
2. [ ] Implement heartbeat monitoring (1s interval)
3. [ ] Implement shard repair loop
4. [ ] Implement rebalancing on node join/leave
5. [ ] Implement background scrubbing

**Testing**:
- [ ] Failure injection tests
- [ ] Recovery time measurement
- [ ] Scrubbing correctness tests

**CI adjustment**:
- [ ] Add chaos testing job

---

### Milestone 3.6: Cluster CLI + Operations

**Deliverable**: Operational tooling

**Tasks**:
1. [ ] Implement `rucket cluster status`
2. [ ] Implement `rucket cluster add-node`
3. [ ] Implement `rucket cluster remove-node`
4. [ ] Implement `rucket cluster rebalance`
5. [ ] Implement rolling upgrade procedure
6. [ ] Admin API for cluster operations

**Testing**:
- [ ] CLI command tests
- [ ] Rolling upgrade test

**CI adjustment**: None required

---

## Phase 4: Geo-Distribution

**Goal**: Cross-region replication

### Milestone 4.1: Hybrid Logical Clocks (Production)

**Deliverable**: Causality tracking across regions

**Files to modify**:
- `crates/rucket-core/src/hlc.rs`
- All storage operations

**Tasks**:
1. [ ] Upgrade HLC to production-ready (clock skew handling)
2. [ ] Integrate HLC into all write operations
3. [ ] Pass HLC in replication stream
4. [ ] Implement clock skew rejection (>500ms)

**Testing**:
- [ ] Clock skew simulation tests
- [ ] Causality ordering tests

**CI adjustment**: None required

---

### Milestone 4.2: Cross-Region Replication

**Deliverable**: S3 CRR API compatibility

**Files to modify**:
- `crates/rucket-geo/` (new crate)
- `crates/rucket-api/src/handlers/bucket.rs`

**Tasks**:
1. [ ] Implement replication configuration API
2. [ ] Implement event-sourced replication
3. [ ] Implement async cross-region streaming
4. [ ] Implement replication status tracking
5. [ ] S3 CRR API compatibility

**Testing**:
- [ ] Multi-region replication tests
- [ ] Conflict generation tests

**CI adjustment**:
- [ ] Add multi-region test environment

---

### Milestone 4.3: Conflict Resolution (LWW)

**Deliverable**: Deterministic conflict resolution

**Files to modify**:
- `crates/rucket-geo/src/conflict.rs` (new)

**Tasks**:
1. [ ] Implement LWW resolution algorithm
2. [ ] Implement tie-breaker (region ID lexicographic)
3. [ ] Implement conflict logging
4. [ ] Implement admin API for conflict inspection
5. [ ] Keep version history for auditing

**Testing**:
- [ ] Conflict simulation tests
- [ ] Resolution determinism tests

**CI adjustment**: None required

---

### Milestone 4.4: Region-Aware Placement

**Deliverable**: Data locality optimization

**Tasks**:
1. [ ] Implement region topology in CRUSH
2. [ ] Implement home_region tracking
3. [ ] Implement geo-aware read routing
4. [ ] Implement regional consistency options

**Testing**:
- [ ] Cross-region latency tests
- [ ] Data locality tests

**CI adjustment**: None required

---

## Phase 5: Community & Governance

**Goal**: Sustainable open-source project

### Milestone 5.1: Foundation Setup

**Tasks**:
1. [ ] Choose foundation model (Apache, Linux Foundation, independent)
2. [ ] Draft governance documents
3. [ ] Set up fiscal sponsorship if needed

---

### Milestone 5.2: Contributor Onboarding

**Tasks**:
1. [ ] Write CONTRIBUTING.md
2. [ ] Create good-first-issue labels
3. [ ] Set up community communication (Discord/Slack)
4. [ ] Onboard first external maintainer

---

## CI/CD Evolution Plan

### Current CI (Phase 1)
```yaml
jobs: [changes, check, fmt, clippy, test, deny, docs, coverage]
```

### Phase 2 Additions
```yaml
+ fuzz-testing:      # Corruption/chaos tests (weekly)
+ s3-compat-gate:    # S3 compatibility threshold (required)
```

### Phase 3 Additions
```yaml
+ cluster-tests:     # 3-node cluster integration
+ chaos-testing:     # Failure injection
+ performance-gate:  # Regression detection
```

### Phase 4 Additions
```yaml
+ multi-region:      # Cross-region replication tests
+ conflict-tests:    # Conflict resolution verification
```

---

## Plan Adjustment Checkpoints

After each milestone, evaluate:

1. **Does the implementation match the architecture?** → Update docs/architecture.md
2. **Did we discover new requirements?** → Add to backlog
3. **Are tests passing?** → Fix before proceeding
4. **Is performance acceptable?** → Profile and optimize
5. **Are there security concerns?** → Security review

---

## Dependency Graph

```
Phase 1.1 (Data Model) ─┬─→ Phase 1.2 (Placement)
                        ├─→ Phase 1.3 (Events)
                        └─→ Phase 1.4 (S3 Compat)
                              ↓
                        Phase 1.5 (Baseline)
                              ↓
Phase 2.1 (Object Lock) ─┬─→ Phase 2.2 (SSE)
Phase 2.3 (Policies)     │
Phase 2.4 (WAL Hardening)┴─→ Phase 2.5 (Docs)
                              ↓
Phase 3.1 (Raft) ─────────→ Phase 3.2 (CRUSH)
                              ↓
                        Phase 3.3 (Erasure Coding)
                              ↓
                        Phase 3.4 (Replication)
                              ↓
                        Phase 3.5 (Self-Healing)
                              ↓
                        Phase 3.6 (Cluster CLI)
                              ↓
Phase 4.1 (HLC Prod) ─────→ Phase 4.2 (CRR)
                              ↓
                        Phase 4.3 (Conflict)
                              ↓
                        Phase 4.4 (Geo Placement)
                              ↓
                        Phase 5 (Community)
```

---

## Immediate Next Steps

1. **Milestone 1.1**: Add forward-compatible fields to ObjectMetadata
2. **Milestone 1.1**: Implement HLC in rucket-core
3. **Milestone 1.2**: Create PlacementPolicy trait with SingleNodePlacement
4. **Milestone 1.3**: Create StorageEvent enum and emit from LocalStorage

**Estimated first deliverable**: Data model ready for distributed future
