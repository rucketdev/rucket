#!/usr/bin/env bash

# S3 Compatibility Test Runner
#
# This script runs S3 compatibility tests against Rucket using various test suites.
# Currently supported: minio (MinIO Mint)
#
# Usage: ./scripts/s3-compat-tests.sh [SUITE] [OPTIONS]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
S3_COMPAT_DIR="$SCRIPT_DIR/s3-compat"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

usage() {
    cat << EOF
S3 Compatibility Test Runner for Rucket

Usage: $0 [SUITE] [OPTIONS]

Suites:
  minio     Run MinIO Mint tests (default)
  ceph      Run Ceph S3 compatibility tests

Options (passed to suite runner):
  --build-image       Build test image from source (gets latest fixes)
  --image NAME        Use specific Docker image
  --timeout SECS      Timeout in seconds (default: 1800)
  -h, --help          Show this help message

Environment Variables:
  RUCKET_ENDPOINT     Rucket S3 endpoint (default: http://127.0.0.1:9000)
  RUCKET_ACCESS_KEY   S3 access key (default: rucket)
  RUCKET_SECRET_KEY   S3 secret key (default: rucket123)

Examples:
  $0                              # Run minio tests with defaults
  $0 minio                        # Run minio tests explicitly
  $0 minio --build-image          # Build mint from source and run
  $0 minio --timeout 3600         # Run with 1 hour timeout

Requirements:
  - Docker or Podman
  - Rucket must be running before executing tests

Reports are saved to: target/s3-compat-reports/
EOF
    exit 0
}

check_rucket_running() {
    local endpoint="${RUCKET_ENDPOINT:-http://127.0.0.1:9000}"

    log_info "Checking if Rucket is running at $endpoint..."

    if curl -s --connect-timeout 5 "$endpoint" > /dev/null 2>&1; then
        log_success "Rucket is running at $endpoint"
        return 0
    else
        log_error "Cannot connect to Rucket at $endpoint"
        log_info "Please start Rucket first: cargo run --release -- serve"
        exit 1
    fi
}

run_suite() {
    local suite="$1"
    shift

    case "$suite" in
        minio|mint)
            local runner="$S3_COMPAT_DIR/minio-runner.sh"
            if [[ ! -x "$runner" ]]; then
                chmod +x "$runner"
            fi
            exec "$runner" "$@"
            ;;
        ceph|s3-tests)
            local runner="$S3_COMPAT_DIR/ceph-runner.sh"
            if [[ ! -x "$runner" ]]; then
                chmod +x "$runner"
            fi
            exec "$runner" "$@"
            ;;
        *)
            log_error "Unknown test suite: $suite"
            log_info "Supported suites: minio, ceph"
            exit 1
            ;;
    esac
}

# Parse first argument as suite or help
SUITE="minio"
if [[ $# -gt 0 ]]; then
    case "$1" in
        -h|--help|help)
            usage
            ;;
        -*)
            # First arg is an option, keep default suite
            ;;
        *)
            # First arg is suite name
            SUITE="$1"
            shift
            ;;
    esac
fi

# Main
log_info "S3 Compatibility Test Runner"
log_info "============================="
log_info "Suite: $SUITE"
echo ""

check_rucket_running
run_suite "$SUITE" "$@"
