#!/usr/bin/env bash
# Copyright 2026 Rucket Dev
# SPDX-License-Identifier: Apache-2.0

# Run benchmarks and generate SVG graphs.
#
# Usage:
#   ./scripts/run-benchmarks.sh         # Run all benchmarks
#   ./scripts/run-benchmarks.sh quick   # Run only profile benchmarks
#   ./scripts/run-benchmarks.sh graphs  # Generate graphs from existing results

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

run_benchmarks() {
    local filter="${1:-}"

    if [[ -n "$filter" ]]; then
        log_info "Running benchmarks matching: $filter"
        cargo bench --bench throughput -- "$filter"
    else
        log_info "Running all benchmarks..."
        cargo bench --bench throughput
    fi
}

generate_graphs() {
    log_info "Generating graphs..."

    if [[ ! -d "target/criterion" ]]; then
        log_error "No benchmark results found. Run benchmarks first."
        exit 1
    fi

    cargo run --features bench-graph --bin bench-graph --release

    log_info "Graphs generated in docs/benchmarks/graphs/"
}

show_help() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  (none)    Run all benchmarks and generate graphs"
    echo "  quick     Run only profile_put and profile_get benchmarks"
    echo "  graphs    Generate graphs from existing benchmark results"
    echo "  help      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0              # Run full benchmark suite"
    echo "  $0 quick        # Quick benchmark run"
    echo "  $0 graphs       # Regenerate graphs only"
}

main() {
    local command="${1:-all}"

    case "$command" in
        help|--help|-h)
            show_help
            ;;
        quick)
            run_benchmarks "profile_"
            generate_graphs
            ;;
        graphs)
            generate_graphs
            ;;
        all|"")
            run_benchmarks
            generate_graphs
            ;;
        *)
            log_error "Unknown command: $command"
            show_help
            exit 1
            ;;
    esac

    log_info "Done!"
}

main "$@"
