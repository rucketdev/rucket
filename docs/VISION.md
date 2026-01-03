# Vision

## Why

The S3-compatible storage landscape has a gap:

| Project | License | Governance | S3 Features | Distributed | Simple |
|---------|---------|------------|-------------|-------------|--------|
| MinIO | AGPL | Single vendor | Full | Yes | Yes |
| RustFS | Apache | Single vendor | Full | Yes | Yes |
| Garage | AGPL | Non-profit | Partial | Yes | Yes |
| CubeFS | Apache | CNCF | Partial | Yes | No |
| Ceph | LGPL | Foundation | Full | Yes | No |
| SeaweedFS | Apache | Single vendor | Full | Yes | Medium |

**MinIO** is AGPL but controlled by a single company that has [stripped features](https://www.futuriom.com/articles/news/minio-faces-fallout-for-stripping-features-from-web-gui/2025/06) from the open source version and put the project in maintenance mode.

**RustFS** is Apache 2.0 with single-vendor control—the same pattern that led to MinIO's issues.

**Garage** is community-governed but [explicitly rejects](https://garagehq.deuxfleurs.fr/documentation/design/goals/) feature completeness: no versioning, no tagging, no erasure coding, no performance optimization.

**CubeFS** is CNCF graduated (Jan 2025), but multi-protocol (POSIX/HDFS/S3) means complexity overkill for S3-only users. S3 is bolted-on, not native—versioning/tagging support unclear. Apache 2.0 offers no copyleft protection.

**Ceph** requires significant operational complexity for simple use cases—designed for petabyte-scale, not self-hosters.

**SeaweedFS** is Apache 2.0 single-vendor with less mature S3 compatibility than MinIO.

Rucket aims to be: community-governed, feature-complete, simple to operate.

---

## Goals

### G1: Open Source

- AGPL v3 license
- Foundation governance (no single-vendor control)
- Codebase simple enough to audit and fork

### G2: S3 Compatibility

Drop-in replacement, not "S3-ish". Target: 90%+ on Ceph s3-tests.

| Feature | Status |
|---------|--------|
| Object versioning | Done |
| Object/bucket tagging | Done |
| Checksums (CRC32, CRC32C, SHA-1, SHA-256) | Done |
| Multipart uploads | Done |
| Presigned URLs | Done |
| Conditional requests | Done |
| Range requests | Done |
| CORS | Done |
| Bucket policies (enforced) | Planned |
| Object Lock | Planned |
| SSE-S3/SSE-KMS | Planned |

### G3: Performance

Competitive with MinIO on single-node workloads. Benchmark-driven development.

### G4: Durability

Data safety is non-negotiable. Write-ahead logging, crash recovery, no silent data loss.

### G5: Distributed

Single-node first, distributed later. Design decisions today must not preclude:

- Erasure coding
- High availability (automatic failover)
- Horizontal scaling
- Storage tiering

---

## Non-Goals

- Multi-protocol (POSIX, HDFS, NFS)
- Geo-distribution
- Paid enterprise edition
- Kubernetes-only deployment

---

## Roadmap

### Phase 1: S3 Parity (Current)

- Fix compatibility issues (signature validation, versioning edge cases)
- Pass 80%+ Ceph s3-tests
- Enforce bucket policies
- Baseline performance benchmarks

### Phase 2: Production

- Object Lock (compliance/governance modes)
- SSE-S3 encryption
- Hardened crash recovery
- Production deployment documentation

### Phase 3: Distributed

- Erasure coding
- Storage tiering
- Active-passive HA
- Multi-node clustering

### Phase 4: Governance

- Decision-making process documentation
- Contributor guide
- Foundation setup

---

## Principles

1. **Quality over speed.** Don't ship broken things.
2. **Compatibility is the product.** If an AWS SDK doesn't work, it's a bug.
3. **Simple over clever.** Readable code wins.
4. **Community over company.** Decisions benefit users, not a business model.
