#!/usr/bin/env bash
# Copyright 2026 Rucket Dev
# SPDX-License-Identifier: Apache-2.0

# S3 Compatibility Test Runner
# Usage: ./scripts/s3-compat-tests.sh [ceph|mint|all]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
S3_COMPAT_DIR="$SCRIPT_DIR/s3-compat"
VENDOR_DIR="$PROJECT_ROOT/.s3-compat-tests"
REPORTS_DIR="$PROJECT_ROOT/target/s3-compat-reports"

# Default configuration
RUCKET_ENDPOINT="${RUCKET_ENDPOINT:-http://127.0.0.1:9000}"
RUCKET_ACCESS_KEY="${RUCKET_ACCESS_KEY:-rucket}"
RUCKET_SECRET_KEY="${RUCKET_SECRET_KEY:-rucket123}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

usage() {
    cat << EOF
S3 Compatibility Test Runner for Rucket

Usage: $0 [OPTIONS] <test-suite>

Test Suites:
  ceph        Run ceph/s3-tests (Python, Boto3)
  mint        Run minio/mint (Docker, multi-SDK)
  all         Run all test suites

Options:
  -e, --endpoint URL    Rucket endpoint (default: $RUCKET_ENDPOINT)
  -a, --access-key KEY  Access key (default: $RUCKET_ACCESS_KEY)
  -s, --secret-key KEY  Secret key (default: $RUCKET_SECRET_KEY)
  -c, --clean           Clean vendor directory before running
  -h, --help            Show this help message

Environment Variables:
  RUCKET_ENDPOINT       Rucket S3 endpoint URL
  RUCKET_ACCESS_KEY     S3 access key
  RUCKET_SECRET_KEY     S3 secret key
  RUCKET_PID            PID of running Rucket server (for auto-start)

Examples:
  $0 ceph                          # Run ceph/s3-tests against local Rucket
  $0 mint                          # Run minio/mint against local Rucket
  $0 --endpoint http://localhost:9000 all  # Run all tests

Reports are saved to: $REPORTS_DIR
EOF
    exit 0
}

check_dependencies() {
    local missing=()

    if ! command -v git &> /dev/null; then
        missing+=("git")
    fi

    if [[ "$1" == "ceph" || "$1" == "all" ]]; then
        if ! command -v python3 &> /dev/null; then
            missing+=("python3")
        fi
        if ! command -v pip3 &> /dev/null && ! command -v pip &> /dev/null; then
            missing+=("pip3")
        fi
    fi

    if [[ "$1" == "mint" || "$1" == "all" ]]; then
        if ! command -v docker &> /dev/null && ! command -v podman &> /dev/null; then
            missing+=("docker or podman")
        fi
    fi

    if [[ ${#missing[@]} -gt 0 ]]; then
        log_error "Missing dependencies: ${missing[*]}"
        exit 1
    fi
}

check_rucket_running() {
    log_info "Checking if Rucket is running at $RUCKET_ENDPOINT..."

    if curl -s --connect-timeout 5 "$RUCKET_ENDPOINT" > /dev/null 2>&1; then
        log_success "Rucket is running at $RUCKET_ENDPOINT"
        return 0
    else
        log_error "Cannot connect to Rucket at $RUCKET_ENDPOINT"
        log_info "Please start Rucket first: cargo run --release"
        exit 1
    fi
}

setup_directories() {
    mkdir -p "$VENDOR_DIR"
    mkdir -p "$REPORTS_DIR"
}

run_ceph_tests() {
    log_info "Running ceph/s3-tests..."

    source "$S3_COMPAT_DIR/ceph-runner.sh"

    ceph_setup "$VENDOR_DIR"
    ceph_run "$RUCKET_ENDPOINT" "$RUCKET_ACCESS_KEY" "$RUCKET_SECRET_KEY" "$REPORTS_DIR"
}

run_mint_tests() {
    log_info "Running minio/mint..."

    source "$S3_COMPAT_DIR/mint-runner.sh"

    mint_run "$RUCKET_ENDPOINT" "$RUCKET_ACCESS_KEY" "$RUCKET_SECRET_KEY" "$REPORTS_DIR"
}

generate_summary() {
    local report_file="$REPORTS_DIR/summary.md"
    local timestamp=$(date -u +"%Y-%m-%d %H:%M:%S UTC")

    cat > "$report_file" << EOF
# S3 Compatibility Test Summary

**Date:** $timestamp
**Endpoint:** $RUCKET_ENDPOINT

## Test Results

EOF

    if [[ -f "$REPORTS_DIR/ceph-results.json" ]]; then
        echo "### ceph/s3-tests" >> "$report_file"
        echo "" >> "$report_file"
        if command -v jq &> /dev/null; then
            local passed=$(jq -r '.passed // 0' "$REPORTS_DIR/ceph-results.json" 2>/dev/null || echo "N/A")
            local failed=$(jq -r '.failed // 0' "$REPORTS_DIR/ceph-results.json" 2>/dev/null || echo "N/A")
            local skipped=$(jq -r '.skipped // 0' "$REPORTS_DIR/ceph-results.json" 2>/dev/null || echo "N/A")
            echo "- Passed: $passed" >> "$report_file"
            echo "- Failed: $failed" >> "$report_file"
            echo "- Skipped: $skipped" >> "$report_file"
        else
            echo "See ceph-results.json for details" >> "$report_file"
        fi
        echo "" >> "$report_file"
    fi

    if [[ -f "$REPORTS_DIR/mint-results.json" ]]; then
        echo "### minio/mint" >> "$report_file"
        echo "" >> "$report_file"
        if command -v jq &> /dev/null; then
            local passed=$(jq -s '[.[] | select(.status == "PASS")] | length' "$REPORTS_DIR/mint-results.json" 2>/dev/null || echo "N/A")
            local failed=$(jq -s '[.[] | select(.status == "FAIL")] | length' "$REPORTS_DIR/mint-results.json" 2>/dev/null || echo "N/A")
            local na=$(jq -s '[.[] | select(.status == "NA")] | length' "$REPORTS_DIR/mint-results.json" 2>/dev/null || echo "N/A")
            echo "- Passed: $passed" >> "$report_file"
            echo "- Failed: $failed" >> "$report_file"
            echo "- N/A: $na" >> "$report_file"
        else
            echo "See mint-results.json for details" >> "$report_file"
        fi
        echo "" >> "$report_file"
    fi

    log_success "Summary saved to $report_file"
}

# Parse arguments
CLEAN=false
TEST_SUITE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--endpoint)
            RUCKET_ENDPOINT="$2"
            shift 2
            ;;
        -a|--access-key)
            RUCKET_ACCESS_KEY="$2"
            shift 2
            ;;
        -s|--secret-key)
            RUCKET_SECRET_KEY="$2"
            shift 2
            ;;
        -c|--clean)
            CLEAN=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        ceph|mint|all)
            TEST_SUITE="$1"
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

if [[ -z "$TEST_SUITE" ]]; then
    log_error "No test suite specified"
    usage
fi

# Main execution
log_info "S3 Compatibility Test Runner"
log_info "=============================="

check_dependencies "$TEST_SUITE"
setup_directories

if [[ "$CLEAN" == true ]]; then
    log_info "Cleaning vendor directory..."
    rm -rf "$VENDOR_DIR"
    mkdir -p "$VENDOR_DIR"
fi

check_rucket_running

case "$TEST_SUITE" in
    ceph)
        run_ceph_tests
        ;;
    mint)
        run_mint_tests
        ;;
    all)
        run_ceph_tests
        run_mint_tests
        ;;
esac

generate_summary

log_success "All tests completed! Reports saved to $REPORTS_DIR"
