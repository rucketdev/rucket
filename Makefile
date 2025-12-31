.PHONY: setup fmt fmt-check check clippy test test-integration lint doc deny bench all \
       s3-compat s3-compat-minio s3-compat-build s3-compat-clean

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

# Run all lint checks (used by pre-commit hook)
lint: fmt-check clippy

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
# Runs S3 compatibility tests against Rucket using various test suites.
# Rucket must be running before executing these tests.
#
# Usage:
#   make s3-compat              # Run tests with default suite (minio)
#   make s3-compat-minio        # Run MinIO Mint tests
#   make s3-compat-build        # Build mint from source and run
#   make s3-compat-clean        # Clean test artifacts
#
# Environment variables:
#   RUCKET_ENDPOINT    - Rucket S3 endpoint (default: http://127.0.0.1:9000)
#   RUCKET_ACCESS_KEY  - S3 access key (default: rucket)
#   RUCKET_SECRET_KEY  - S3 secret key (default: rucket123)
#
# Requirements:
#   - Docker or Podman
# ============================================================================

# Run S3 compatibility tests (default: minio)
s3-compat:
	./scripts/s3-compat-tests.sh minio

# Run MinIO Mint tests
s3-compat-minio:
	./scripts/s3-compat-tests.sh minio

# Build mint image from source and run tests (gets latest fixes)
s3-compat-build:
	./scripts/s3-compat-tests.sh minio --build-image

# Clean S3 compatibility test artifacts
s3-compat-clean:
	rm -rf target/s3-compat-reports
