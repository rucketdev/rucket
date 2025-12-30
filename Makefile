# Copyright 2026 Rucket Dev
# SPDX-License-Identifier: Apache-2.0

.PHONY: setup fmt fmt-check check clippy test test-integration license-headers lint doc deny all

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

# Run all CI checks locally
all: lint test doc deny
