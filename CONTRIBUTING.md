# Contributing to Rucket

Thank you for your interest in contributing to Rucket!

## Developer Certificate of Origin

By contributing to this project, you certify that:

1. The contribution was created by you and you have the right to submit it under the Apache-2.0 license.
2. You understand and agree that your contribution is public and may be redistributed.

**Sign-off your commits:**

```bash
git commit -s -m "Your commit message"
```

This adds a `Signed-off-by` line to your commit message, certifying that you wrote or have the right to submit the code.

## Development Setup

```bash
# Clone repository
git clone https://github.com/rucketdev/rucket.git
cd rucket

# Install development tools
rustup component add rustfmt clippy

# Build
cargo build

# Run tests
cargo test

# Run lints
cargo clippy --all-targets --all-features
cargo fmt --all -- --check
```

## Running Locally

```bash
# Run with default configuration
cargo run -- serve

# Run with debug logging
RUST_LOG=debug cargo run -- serve

# Run with custom config
cargo run -- serve --config rucket.toml
```

## Testing with AWS CLI

```bash
# Set credentials
export AWS_ACCESS_KEY_ID=rucket
export AWS_SECRET_ACCESS_KEY=rucket123

# Test operations
aws --endpoint-url http://localhost:9000 s3 mb s3://test-bucket
aws --endpoint-url http://localhost:9000 s3 cp README.md s3://test-bucket/
aws --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/
```

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Ensure tests pass (`cargo test`)
5. Ensure lints pass (`cargo clippy`, `cargo fmt --check`)
6. Commit with sign-off (`git commit -s`)
7. Push to your fork
8. Open a Pull Request

## Code Style

- Follow `rustfmt` formatting (run `cargo fmt`)
- Address all `clippy` warnings
- Write documentation for public APIs
- Include tests for new functionality
- Keep commits atomic and well-described

## Project Structure

```
rucket/
├── crates/
│   ├── rucket/           # Main binary and CLI
│   ├── rucket-api/       # S3 API handlers (Axum-based REST API)
│   ├── rucket-core/      # Shared types, HLC, error handling
│   ├── rucket-storage/   # Storage backend (local, WAL, metadata)
│   ├── rucket-placement/ # CRUSH placement algorithm
│   ├── rucket-erasure/   # Reed-Solomon erasure coding (8+4)
│   ├── rucket-replication/ # Primary-backup replication
│   ├── rucket-consensus/ # Raft consensus (openraft-based)
│   ├── rucket-cluster/   # Failure detection, repair, rebalancing
│   └── rucket-geo/       # Cross-region replication, geo placement
├── docs/                 # Documentation (architecture, deployment)
├── scripts/              # Build and test scripts
└── .github/              # CI/CD workflows
```

### Crate Dependencies

```
rucket-core (base types, HLC)
    ↓
rucket-placement (CRUSH) ←── rucket-erasure (Reed-Solomon)
    ↓
rucket-storage (WAL, metadata)
    ↓
rucket-replication (primary-backup) ←── rucket-consensus (Raft)
    ↓
rucket-cluster (failure detection)
    ↓
rucket-geo (cross-region) ←── rucket-api (S3 handlers)
    ↓
rucket (binary)
```

## Good First Issues

Looking for a place to start? Issues labeled `good-first-issue` are great entry points:

### Easy Contributions
- **Documentation**: Improve docs, add examples, fix typos
- **Tests**: Add unit tests for edge cases, improve coverage
- **Error messages**: Make error messages more helpful
- **Logging**: Add debug/trace logging to help troubleshooting

### Intermediate Contributions
- **S3 compatibility**: Implement missing S3 API features
- **Metrics**: Add Prometheus metrics to new subsystems
- **CLI improvements**: Add new commands or improve existing ones
- **Performance**: Profile and optimize hot paths

### Advanced Contributions
- **Consensus**: Improve Raft integration and testing
- **Placement**: Enhance CRUSH algorithm for better distribution
- **Replication**: Add new replication strategies
- **Geo-distribution**: Improve cross-region features

## Reporting Issues

Use GitHub Issues for bug reports and feature requests. Please include:

- Rucket version (`rucket version`)
- Operating system and version
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs (with `RUST_LOG=debug`)

## Architecture Overview

For a deeper understanding of the project architecture, see:

- [`docs/architecture.md`](docs/architecture.md) - System design and components
- [`docs/ROADMAP.md`](docs/ROADMAP.md) - Development roadmap and milestones
- [`docs/DURABILITY.md`](docs/DURABILITY.md) - Durability guarantees
- [`docs/s3-compatibility.md`](docs/s3-compatibility.md) - S3 API compatibility status

## Questions?

Feel free to open a Discussion on GitHub for questions about contributing or the project in general.
