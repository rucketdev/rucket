# Roadmap

Current test coverage: **472 passing** / 530 ignored (47% S3 API coverage)

---

## Phase 1: Quick Wins

Low complexity, high value. Target: 54% coverage.

| Feature | Tests | Status |
|---------|-------|--------|
| CORS support | 17 | ⬜ |
| Object tagging persistence | 9 | ⬜ |
| Bucket tagging persistence | 6 | ⬜ |
| Tag validation (length, count) | 6 | ⬜ |
| GetObjectAttributes | 10 | ⬜ |
| Checksum validation (CRC32, SHA) | 13 | ⬜ |
| Multipart metadata (cache-control, etc.) | 3 | ⬜ |
| List pagination edge cases | 5 | ⬜ |

## Phase 2: Access Control

Foundation for enterprise use. Target: 63% coverage.

| Feature | Tests | Status |
|---------|-------|--------|
| Canned ACLs | 36 | ⬜ |
| Bucket policies | 37 | ⬜ |
| Anonymous/public access | 15 | ⬜ |

## Phase 3: Encryption

Security-critical features. Target: 68% coverage.

| Feature | Tests | Status |
|---------|-------|--------|
| SSE-S3 (server-managed keys) | 16 | ⬜ |
| SSE-C (customer-provided keys) | 21 | ⬜ |
| Bucket default encryption | 14 | ⬜ |

## Phase 4: Data Management

Compliance and lifecycle. Target: 75% coverage.

| Feature | Tests | Status |
|---------|-------|--------|
| Lifecycle rules (expiration, transitions) | 29 | ⬜ |
| Object Lock (WORM) | 24 | ⬜ |
| Object retention | 4 | ⬜ |
| Legal hold | 4 | ⬜ |
| Storage classes | 14 | ⬜ |

## Phase 5: Advanced

Complex features for specific use cases. Target: 100% coverage.

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
| Presigned URLs | 26 | ⬜ |

---

## Future / Experimental

Long-term features for scale, performance, and enterprise deployment.

### Cluster Mode

| Phase | Feature | Description |
|-------|---------|-------------|
| 1 | High Availability (HA) | Active/passive replication for automatic failover |
| 2 | Data Partitioning | Consistent hashing to distribute data across nodes |

### Storage Performance

| Feature | Description |
|---------|-------------|
| Direct NVMe access | Bypass filesystem for raw block devices |
| io_uring support | Linux async I/O for improved throughput |
| SPDK integration | Storage Performance Development Kit for ultra-low latency |

---

## Complexity Guide

| Level | Estimate | Description |
|-------|----------|-------------|
| Low | < 1 day | Isolated changes |
| Medium | 1-3 days | Multiple files, new modules |
| High | 3-7 days | New subsystem |
| Very High | 1-2 weeks | Major architecture |

## Architectural Decisions Needed

- [ ] **Policy engine**: JSON policy evaluation approach
- [ ] **Key management**: Built-in vs external KMS
- [ ] **Background jobs**: Lifecycle rule execution model
- [ ] **Events**: Internal bus vs external queue integration
- [ ] **Multi-tenancy**: Account isolation model

---

## Contributing

Pick an unchecked item, open an issue to discuss approach, submit PR. See [CONTRIBUTING.md](../CONTRIBUTING.md).
