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
│   ├── rucket/           # Main binary
│   ├── rucket-api/       # S3 API handlers
│   ├── rucket-storage/   # Storage backend
│   └── rucket-core/      # Shared types
├── tests/                # Integration tests
├── benches/              # Benchmarks
└── docs/                 # Documentation
```

## Reporting Issues

Use GitHub Issues for bug reports and feature requests. Please include:

- Rucket version (`rucket version`)
- Operating system and version
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs (with `RUST_LOG=debug`)

## Questions?

Feel free to open a Discussion on GitHub for questions about contributing or the project in general.
