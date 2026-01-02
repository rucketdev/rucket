# Rucket Makefile
# ================
# Run `make` or `make help` to see available targets.

# Shell configuration
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables --no-builtin-rules

# Variables
VERSION := $(shell grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)
CARGO := cargo
DOCKER := docker

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@echo "Rucket v$(VERSION)"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ============================================================================
# Development
# ============================================================================

.PHONY: setup
setup: ## Install development dependencies (lefthook, cargo-edit, git-cliff)
	lefthook install
	@command -v cargo-set-version >/dev/null 2>&1 || $(CARGO) install cargo-edit
	@command -v git-cliff >/dev/null 2>&1 || $(CARGO) install git-cliff

.PHONY: fmt
fmt: ## Format code
	$(CARGO) fmt --all -- --config imports_granularity=Module --config group_imports=StdExternalCrate

.PHONY: fmt-check
fmt-check: ## Check code formatting
	$(CARGO) fmt --all -- --check --config imports_granularity=Module --config group_imports=StdExternalCrate

.PHONY: check
check: ## Run cargo check
	$(CARGO) check --all-targets --all-features

.PHONY: clippy
clippy: ## Run clippy lints
	$(CARGO) clippy --all-targets --all-features -- -D warnings

.PHONY: test
test: ## Run all tests
	$(CARGO) test --all-features

.PHONY: test-integration
test-integration: ## Run integration tests (single-threaded)
	$(CARGO) test --test '*' -- --test-threads=1

.PHONY: lint
lint: fmt-check clippy ## Run all lints (format + clippy)

.PHONY: doc
doc: ## Build documentation
	$(CARGO) doc --no-deps --all-features

.PHONY: deny
deny: ## Run cargo-deny checks
	$(CARGO) deny check

.PHONY: bench
bench: ## Run benchmarks
	./scripts/run-benchmarks.sh

.PHONY: coverage
coverage: ## Generate code coverage report
	./scripts/coverage.sh

.PHONY: coverage-open
coverage-open: coverage ## Generate and open coverage report
	open target/coverage/html/index.html 2>/dev/null || xdg-open target/coverage/html/index.html

# ============================================================================
# Build
# ============================================================================

.PHONY: build
build: ## Build release binary
	$(CARGO) build --release

.PHONY: run
run: ## Run the server
	$(CARGO) run --release -- serve

.PHONY: install
install: ## Install to ~/.cargo/bin
	$(CARGO) install --path crates/rucket

.PHONY: clean
clean: ## Remove build artifacts
	$(CARGO) clean
	rm -rf target/coverage target/s3-compat-reports

# ============================================================================
# Release
# ============================================================================

.PHONY: release-patch
release-patch: _check-clean ## Create a patch release (0.0.X)
	@cargo set-version --bump patch
	@$(MAKE) _do-release

.PHONY: release-minor
release-minor: _check-clean ## Create a minor release (0.X.0)
	@cargo set-version --bump minor
	@$(MAKE) _do-release

.PHONY: release-major
release-major: _check-clean ## Create a major release (X.0.0)
	@cargo set-version --bump major
	@$(MAKE) _do-release

.PHONY: changelog
changelog: ## Generate changelog (without release)
	git cliff -o CHANGELOG.md

.PHONY: _check-clean
_check-clean:
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "Error: Working directory is not clean. Commit or stash changes first."; \
		exit 1; \
	fi

.PHONY: _do-release
_do-release:
	$(eval NEW_VERSION := $(shell grep '^version' Cargo.toml | head -1 | cut -d'"' -f2))
	@echo "Creating release v$(NEW_VERSION)..."
	git cliff -o CHANGELOG.md
	$(CARGO) check
	git add Cargo.toml Cargo.lock CHANGELOG.md
	git commit -m "chore(release): v$(NEW_VERSION)"
	git tag "v$(NEW_VERSION)"
	@echo ""
	@echo "Release v$(NEW_VERSION) ready!"
	@echo "Run 'git push && git push --tags' to publish."

# ============================================================================
# Docker
# ============================================================================

.PHONY: docker-build
docker-build: ## Build Docker image locally
	$(DOCKER) build -t rucket:$(VERSION) -t rucket:latest .

.PHONY: docker-run
docker-run: ## Run Docker container
	$(DOCKER) run -p 9000:9000 -v rucket-data:/data rucket:latest

# ============================================================================
# CI
# ============================================================================

.PHONY: all
all: lint test doc deny ## Run all CI checks locally

# ============================================================================
# S3 Compatibility Tests
# ============================================================================

.PHONY: s3-compat
s3-compat: ## Run S3 compatibility tests (MinIO)
	./scripts/s3-compat-tests.sh minio

.PHONY: s3-compat-minio
s3-compat-minio: ## Run MinIO Mint tests
	./scripts/s3-compat-tests.sh minio

.PHONY: s3-compat-ceph
s3-compat-ceph: ## Run Ceph s3-tests
	./scripts/s3-compat-tests.sh ceph

.PHONY: s3-compat-build
s3-compat-build: ## Build mint image from source and run
	./scripts/s3-compat-tests.sh minio --build-image

.PHONY: s3-compat-clean
s3-compat-clean: ## Clean S3 test artifacts
	rm -rf target/s3-compat-reports
