# Roadmap

---

## Phase 1: Foundation

Core platform capabilities. Solid base for production use.

### Completed

| Feature | Status |
|---------|--------|
| Core S3 API (buckets, objects, multipart) | ✅ |
| Object versioning (with delete markers) | ✅ |
| WAL crash recovery (light/full modes) | ✅ |
| CRC32C checksums with verification | ✅ |
| Durability presets (Performance/Balanced/Durable) | ✅ |
| Sync strategies (None/Periodic/Threshold/Always) | ✅ |
| Prometheus metrics | ✅ |
| AWS Signature V4 authentication | ✅ |
| Conditional requests (If-Match, If-None-Match, etc.) | ✅ |
| Range requests | ✅ |
| User metadata (x-amz-meta-*) | ✅ |
| Batch delete operations | ✅ |
| Copy object with metadata directive | ✅ |
| TLS/HTTPS support | ✅ |
| Presigned URLs | ✅ |
| Object tagging persistence | ✅ |
| CORS support | ✅ |
| Bucket tagging persistence | ✅ |
| Tag validation (length, count) | ✅ |
| GetObjectAttributes | ✅ |

### To Complete - Critical

| Feature | Tests | Status |
|---------|-------|--------|
| High Availability (active/passive) | ~20 | ⬜ |
| S3 compatibility test suite (Rust port) | 500+ | ⬜ |

### To Complete - Quick Wins

| Feature | Tests | Status |
|---------|-------|--------|
| Checksum validation (CRC32, SHA) | 13 | ⬜ |
| Multipart metadata (cache-control, etc.) | 3 | ⬜ |
| List pagination edge cases | 5 | ⬜ |

---

## Phase 2: Scale & Performance

Horizontal scaling and I/O optimization.

| Feature | Description | Status |
|---------|-------------|--------|
| HA cross-site replication | Async replication across data centers | ⬜ |
| io_uring support | Linux async I/O for improved throughput | ⬜ |
| Data partitioning | Consistent hashing to distribute data across nodes | ⬜ |

---

## Phase 3: Access Control

Foundation for enterprise use.

| Feature | Tests | Status |
|---------|-------|--------|
| Canned ACLs | 36 | ⬜ |
| Bucket policies | 37 | ⬜ |
| Anonymous/public access | 15 | ⬜ |

---

## Phase 4: Encryption

Security-critical features.

| Feature | Tests | Status |
|---------|-------|--------|
| SSE-S3 (server-managed keys) | 16 | ⬜ |
| SSE-C (customer-provided keys) | 21 | ⬜ |
| Bucket default encryption | 14 | ⬜ |

---

## Phase 5: Data Management

Compliance and lifecycle.

| Feature | Tests | Status |
|---------|-------|--------|
| Lifecycle rules (expiration, transitions) | 29 | ⬜ |
| Object Lock (WORM) | 24 | ⬜ |
| Object retention | 4 | ⬜ |
| Legal hold | 4 | ⬜ |
| Storage classes | 14 | ⬜ |

---

## Phase 6: Advanced

Complex features for specific use cases.

| Feature | Tests | Status |
|---------|-------|--------|
| IAM policies | 29 | ⬜ |
| STS (temporary credentials) | 25 | ⬜ |
| S3 Select (SQL queries) | 18 | ⬜ |
| Event notifications | 20 | ⬜ |
| Bucket replication | 15 | ⬜ |
| SSE-KMS | 16 | ⬜ |
| POST Object (form uploads) | 23 | ⬜ |
| Static website hosting | 15 | ⬜ |
| Glacier restore | 15 | ⬜ |
| Access logging | 13 | ⬜ |

---

## Future / Experimental

Long-term features requiring specialized hardware or significant architecture changes.

| Feature | Description |
|---------|-------------|
| Direct NVMe access | Bypass filesystem for raw block devices |
| SPDK integration | Storage Performance Development Kit for ultra-low latency |

---

## Architectural Decisions Needed

- [ ] **Policy engine**: JSON policy evaluation approach
- [ ] **Key management**: Built-in vs external KMS
- [ ] **Background jobs**: Lifecycle rule execution model
- [ ] **Events**: Internal bus vs external queue integration
- [ ] **Multi-tenancy**: Account isolation model

---

## Complexity Guide

| Level | Estimate | Description |
|-------|----------|-------------|
| Low | < 1 day | Isolated changes |
| Medium | 1-3 days | Multiple files, new modules |
| High | 3-7 days | New subsystem |
| Very High | 1-2 weeks | Major architecture |

---

## Contributing

Pick an unchecked item, open an issue to discuss approach, submit PR. See [CONTRIBUTING.md](../CONTRIBUTING.md).
