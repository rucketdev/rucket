# Changelog

All notable changes to Rucket will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-03

### Bug Fixes

- Fix release workflow version extraction and S3 compat integration by @rucketdev in [#81](https://github.com/rucketdev/rucket/pull/81)
- Add S3 compatibility report to release notes by @rucketdev in [#80](https://github.com/rucketdev/rucket/pull/80)
- Fix S3 compatibility test workflow by @rucketdev in [#79](https://github.com/rucketdev/rucket/pull/79)
- Remove reach from codecov comment layout by @rucketdev in [#74](https://github.com/rucketdev/rucket/pull/74)
- Make periodic sync test more robust for CI by @rucketdev in [#70](https://github.com/rucketdev/rucket/pull/70)
- Cleanup roadmap - remove redundant sections by @rucketdev in [#68](https://github.com/rucketdev/rucket/pull/68)
- Add permissions for security audit workflow by @rucketdev in [#60](https://github.com/rucketdev/rucket/pull/60)
- Improve error handling and logging across the codebase by @rucketdev in [#54](https://github.com/rucketdev/rucket/pull/54)
- Consolidate release workflow into single PR-based flow by @rucketdev in [#46](https://github.com/rucketdev/rucket/pull/46)
- Split release workflow to avoid running on every PR by @rucketdev in [#45](https://github.com/rucketdev/rucket/pull/45)
- Ceph s3-tests configuration and dependencies by @rucketdev in [#36](https://github.com/rucketdev/rucket/pull/36)
- S3 API compatibility improvements by @rucketdev in [#34](https://github.com/rucketdev/rucket/pull/34)
- Initialize redb tables on database open
- Add Owner/Initiator/StorageClass to ListMultipartUploads response
- Handle negative max-keys query parameter by @rucketdev in [#26](https://github.com/rucketdev/rucket/pull/26)
- Increase default body size limit to 5GiB for S3 compatibility by @rucketdev in [#23](https://github.com/rucketdev/rucket/pull/23)
- Use S3-compatible timestamp format with Z suffix by @rucketdev in [#22](https://github.com/rucketdev/rucket/pull/22)
- Use --config for rustfmt unstable options (zero warnings) by @rucketdev in [#14](https://github.com/rucketdev/rucket/pull/14)
- Clean up CI pipeline and fix all warnings by @rucketdev in [#13](https://github.com/rucketdev/rucket/pull/13)
- Apply cargo fmt formatting for stable Rust CI by @rucketdev in [#12](https://github.com/rucketdev/rucket/pull/12)
- Move chart legend to right margin to avoid overlapping bars by @tsouza-squid
- Add threshold-based sync to periodic mode for bounded durability by @tsouza-squid

### CI/CD

- Run CI jobs on release PRs and add S3 compat link by @rucketdev in [#76](https://github.com/rucketdev/rucket/pull/76)
- Add Codecov integration for code coverage reporting by @rucketdev in [#39](https://github.com/rucketdev/rucket/pull/39)
- Bump actions/upload-artifact from 4 to 6 by @dependabot[bot]
- Bump actions/download-artifact from 4 to 7 by @dependabot[bot]
- Bump rustsec/audit-check from 1 to 2 by @dependabot[bot]

### Documentation

- Reorganize roadmap with foundation phase and completed features by @rucketdev in [#51](https://github.com/rucketdev/rucket/pull/51)
- Add roadmap and streamline README by @rucketdev in [#40](https://github.com/rucketdev/rucket/pull/40)
- Add security policy
- Update benchmark methodology and remove combined chart by @tsouza-squid

### Features

- Add 12-factor app environment variable configuration by @rucketdev in [#72](https://github.com/rucketdev/rucket/pull/72)
- Enable 11 more S3 compatibility tests (quick wins) by @rucketdev in [#64](https://github.com/rucketdev/rucket/pull/64)
- Complete Phase 1 quick wins by @rucketdev in [#62](https://github.com/rucketdev/rucket/pull/62)
- Add checksum validation (CRC32, CRC32C, SHA1, SHA256) by @rucketdev in [#61](https://github.com/rucketdev/rucket/pull/61)
- Implement GetObjectAttributes API by @rucketdev in [#59](https://github.com/rucketdev/rucket/pull/59)
- Add tag validation for bucket and object tagging by @rucketdev in [#58](https://github.com/rucketdev/rucket/pull/58)
- Implement bucket tagging persistence by @rucketdev in [#57](https://github.com/rucketdev/rucket/pull/57)
- Implement CORS support for buckets by @rucketdev in [#56](https://github.com/rucketdev/rucket/pull/56)
- Implement presigned URL and SigV4 authentication support by @rucketdev in [#55](https://github.com/rucketdev/rucket/pull/55)
- Implement object tagging persistence by @rucketdev in [#53](https://github.com/rucketdev/rucket/pull/53)
- Add TLS/HTTPS support with comprehensive tests by @rucketdev in [#52](https://github.com/rucketdev/rucket/pull/52)
- Add idle timeout to Threshold sync mode and sample config by @rucketdev in [#49](https://github.com/rucketdev/rucket/pull/49)
- Add CRC32C verification, recovery modes, and durability docs by @rucketdev in [#47](https://github.com/rucketdev/rucket/pull/47)
- Add 'current' option to prepare-release for first release by @rucketdev in [#44](https://github.com/rucketdev/rucket/pull/44)
- Add release infrastructure by @rucketdev in [#41](https://github.com/rucketdev/rucket/pull/41)
- Comprehensive S3 compatibility test suite (1000+ tests) by @rucketdev in [#38](https://github.com/rucketdev/rucket/pull/38)
- Implement S3 quick wins and CORS handlers by @rucketdev in [#37](https://github.com/rucketdev/rucket/pull/37)
- Add ceph/s3-tests compatibility test support by @rucketdev in [#35](https://github.com/rucketdev/rucket/pull/35)
- Add Prometheus metrics and enhanced logging by @rucketdev in [#31](https://github.com/rucketdev/rucket/pull/31)
- Add api-compatibility-mode config with MinIO support
- Implement real multipart upload (replace stub) by @rucketdev in [#27](https://github.com/rucketdev/rucket/pull/27)
- Implement DeleteObjects API (multi-object delete) by @rucketdev in [#25](https://github.com/rucketdev/rucket/pull/25)
- Add S3 compatibility test framework by @rucketdev in [#21](https://github.com/rucketdev/rucket/pull/21)
- Add WAL (Write-Ahead Log) for crash recovery
- Add enhanced durability and performance modes
- Makefile-centric CI with lefthook pre-commit hooks by @rucketdev in [#16](https://github.com/rucketdev/rucket/pull/16)
- Add consistency guarantees with per-key locking and conditional requests by @tsouza-squid
- Add O_DIRECT/F_NOCACHE support for cache-bypass benchmarks by @tsouza-squid

### Miscellaneous

- Pre-release improvements for v0.1.0 by @rucketdev in [#71](https://github.com/rucketdev/rucket/pull/71)
- Skip CI jobs on docs-only changes by @rucketdev in [#69](https://github.com/rucketdev/rucket/pull/69)
- Improve README clarity and accuracy by @rucketdev in [#66](https://github.com/rucketdev/rucket/pull/66)
- Switch to AGPL-3.0 license by @rucketdev in [#65](https://github.com/rucketdev/rucket/pull/65)
- Update ASCII banner by @rucketdev in [#63](https://github.com/rucketdev/rucket/pull/63)
- Remove release-plz (use prepare-release instead) by @rucketdev in [#43](https://github.com/rucketdev/rucket/pull/43)
- Remove per-file license headers by @rucketdev in [#32](https://github.com/rucketdev/rucket/pull/32)
- Update benchmarks and add make bench target

### Refactoring

- Improve Makefile with industry-standard practices by @rucketdev in [#42](https://github.com/rucketdev/rucket/pull/42)
- Restructure S3 compatibility tests with suite support by @rucketdev in [#24](https://github.com/rucketdev/rucket/pull/24)

### Testing

- Add coverage for sync convenience methods and counters by @rucketdev in [#50](https://github.com/rucketdev/rucket/pull/50)
- Add coverage for ChecksumMismatch and uuid_exists_sync by @rucketdev in [#48](https://github.com/rucketdev/rucket/pull/48)

### Bench

- Run GET benchmarks before PUT for consistent results by @tsouza-squid
- Update results from Linux with O_DIRECT cache bypass by @tsouza-squid

### Deps

- Bump metrics-exporter-prometheus from 0.16.2 to 0.18.1 by @dependabot[bot] in [#33](https://github.com/rucketdev/rucket/pull/33)
- Upgrade redb from 2.x to 3.1.0
- Bump quick-xml from 0.37.5 to 0.38.4 by @dependabot[bot]
- Bump criterion from 0.5.1 to 0.8.1 by @dependabot[bot]
- Bump directories from 5.0.1 to 6.0.0 by @dependabot[bot]

