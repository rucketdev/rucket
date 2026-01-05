#!/usr/bin/env bash
# Generate code coverage report for Rucket
set -euo pipefail

# Install cargo-llvm-cov if needed
if ! command -v cargo-llvm-cov &> /dev/null; then
    echo "Installing cargo-llvm-cov..."
    cargo install cargo-llvm-cov
fi

# Run coverage with HTML output
# --ignore-filename-regex excludes test and benchmark code from coverage stats
cargo llvm-cov --all-features --workspace \
    --ignore-filename-regex='benches/|tests/' \
    --html --output-dir target/coverage

echo ""
echo "Coverage report: target/coverage/html/index.html"
