# Copyright 2026 Rucket Dev
# SPDX-License-Identifier: Apache-2.0

.PHONY: setup fmt fmt-check check clippy test test-integration license-headers lint doc deny bench all \
       s3-compat s3-compat-ceph s3-compat-mint s3-compat-clean

# Install lefthook git hooks
setup:
	lefthook install

# Format code with unstable options (imports_granularity, group_imports)
fmt:
	cargo fmt --all -- --config imports_granularity=Module --config group_imports=StdExternalCrate

# Check formatting without modifying files
fmt-check:
	cargo fmt --all -- --check --config imports_granularity=Module --config group_imports=StdExternalCrate

# Run cargo check
check:
	cargo check --all-targets --all-features

# Run clippy with warnings as errors
clippy:
	cargo clippy --all-targets --all-features -- -D warnings

# Run all tests
test:
	cargo test --all-features

# Run integration tests (single-threaded)
test-integration:
	cargo test --test '*' -- --test-threads=1

# Check license headers
license-headers:
	./scripts/check-license-headers.sh

# Run all lint checks (used by pre-commit hook)
lint: fmt-check clippy license-headers

# Build documentation
doc:
	cargo doc --no-deps --all-features

# Run cargo-deny checks
deny:
	cargo deny check

# Run benchmarks and generate graphs
bench:
	./scripts/run-benchmarks.sh

# Run all CI checks locally
all: lint test doc deny

# ============================================================================
# S3 Compatibility Tests
# ============================================================================
# These targets run external S3 compatibility test suites against Rucket.
# Rucket must be running before executing these tests.
#
# Usage:
#   make s3-compat              # Run all S3 compatibility tests
#   make s3-compat-ceph         # Run ceph/s3-tests only
#   make s3-compat-mint         # Run minio/mint only
#   make s3-compat-clean        # Clean downloaded test suites
#
# Environment variables:
#   RUCKET_ENDPOINT    - Rucket S3 endpoint (default: http://127.0.0.1:9000)
#   RUCKET_ACCESS_KEY  - S3 access key (default: rucket)
#   RUCKET_SECRET_KEY  - S3 secret key (default: rucket123)
# ============================================================================

# Default test suite (can be overridden: make s3-compat SUITE=ceph)
SUITE ?= all

# Run S3 compatibility tests
s3-compat:
	./scripts/s3-compat-tests.sh $(SUITE)

# Run ceph/s3-tests
s3-compat-ceph:
	./scripts/s3-compat-tests.sh ceph

# Run minio/mint
s3-compat-mint:
	./scripts/s3-compat-tests.sh mint

# Clean S3 compatibility test artifacts
s3-compat-clean:
	rm -rf .s3-compat-tests
	rm -rf target/s3-compat-reports
