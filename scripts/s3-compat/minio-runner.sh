#!/usr/bin/env bash

# MinIO Mint S3 Compatibility Test Runner
#
# This script runs MinIO Mint tests against Rucket.
# It can use either the official minio/mint image or a locally built one.
#
# Usage: ./minio-runner.sh [OPTIONS]
#
# Options:
#   --build-image    Build mint image from source before running
#   --image NAME     Use specific Docker image (default: minio/mint:latest)
#   --timeout SECS   Timeout in seconds (default: 1800 = 30 min)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
REPORTS_DIR="$PROJECT_ROOT/target/s3-compat-reports"

# Configuration (can be overridden by environment)
RUCKET_ENDPOINT="${RUCKET_ENDPOINT:-http://127.0.0.1:9000}"
RUCKET_ACCESS_KEY="${RUCKET_ACCESS_KEY:-rucket}"
RUCKET_SECRET_KEY="${RUCKET_SECRET_KEY:-rucket123}"

# Options
BUILD_IMAGE=false
MINT_IMAGE="${MINT_IMAGE:-minio/mint:latest}"
TIMEOUT_SECS="${TIMEOUT_SECS:-1800}"

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
MinIO Mint S3 Compatibility Test Runner

Usage: $0 [OPTIONS]

Options:
  --build-image       Build mint image from source (scripts/s3-compat/Dockerfile.minio)
  --image NAME        Docker image to use (default: $MINT_IMAGE)
  --timeout SECS      Timeout in seconds (default: $TIMEOUT_SECS)
  -h, --help          Show this help message

Environment Variables:
  RUCKET_ENDPOINT     Rucket S3 endpoint (default: $RUCKET_ENDPOINT)
  RUCKET_ACCESS_KEY   S3 access key (default: $RUCKET_ACCESS_KEY)
  RUCKET_SECRET_KEY   S3 secret key
  MINT_IMAGE          Docker image to use
  TIMEOUT_SECS        Timeout in seconds

Examples:
  $0                          # Run with official minio/mint image
  $0 --build-image            # Build from source and run
  $0 --image rucket/mint:dev  # Use custom image
EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --build-image)
            BUILD_IMAGE=true
            shift
            ;;
        --image)
            MINT_IMAGE="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT_SECS="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Detect container runtime
get_runtime() {
    if command -v docker &> /dev/null; then
        echo "docker"
    elif command -v podman &> /dev/null; then
        echo "podman"
    else
        log_error "Neither docker nor podman found"
        exit 1
    fi
}

# Build mint image from source
build_image() {
    local runtime="$1"
    local dockerfile="$SCRIPT_DIR/Dockerfile.minio"

    if [[ ! -f "$dockerfile" ]]; then
        log_error "Dockerfile not found: $dockerfile"
        exit 1
    fi

    log_info "Building mint image from source..."
    log_info "This may take 10-20 minutes on first build."

    $runtime build -f "$dockerfile" -t rucket/mint:latest "$PROJECT_ROOT"

    MINT_IMAGE="rucket/mint:latest"
    log_success "Built image: $MINT_IMAGE"
}

# Run mint tests
run_tests() {
    local runtime="$1"
    local results_file="$REPORTS_DIR/mint-results.json"
    local log_file="$REPORTS_DIR/mint-tests.log"

    # Parse endpoint
    local server_endpoint is_https
    if [[ "$RUCKET_ENDPOINT" =~ ^https:// ]]; then
        is_https="1"
        server_endpoint="${RUCKET_ENDPOINT#https://}"
    else
        is_https="0"
        server_endpoint="${RUCKET_ENDPOINT#http://}"
    fi

    # Pull image if not building
    if [[ "$BUILD_IMAGE" != "true" ]]; then
        log_info "Pulling $MINT_IMAGE..."
        $runtime pull "$MINT_IMAGE" 2>/dev/null || true
    fi

    log_info "Running MinIO Mint S3 compatibility tests..."
    log_info "  Image:    $MINT_IMAGE"
    log_info "  Endpoint: $server_endpoint"
    log_info "  HTTPS:    $is_https"
    log_info "  Timeout:  ${TIMEOUT_SECS}s"

    # Create temp dir for logs
    local mint_log_dir
    mint_log_dir=$(mktemp -d)

    # Run mint
    local exit_code=0
    if timeout "$TIMEOUT_SECS" $runtime run --rm --network=host \
        -e "SERVER_ENDPOINT=$server_endpoint" \
        -e "ACCESS_KEY=$RUCKET_ACCESS_KEY" \
        -e "SECRET_KEY=$RUCKET_SECRET_KEY" \
        -e "ENABLE_HTTPS=$is_https" \
        -e "MINT_MODE=core" \
        -v "$mint_log_dir:/mint/log" \
        "$MINT_IMAGE" > "$log_file" 2>&1; then
        log_success "MinIO Mint completed successfully"
    else
        exit_code=$?
        if [[ $exit_code -eq 124 ]]; then
            log_warn "MinIO Mint timed out after $TIMEOUT_SECS seconds"
        else
            log_warn "MinIO Mint completed with exit code: $exit_code"
        fi
    fi

    # Copy results
    if [[ -f "$mint_log_dir/log.json" ]]; then
        cp "$mint_log_dir/log.json" "$results_file"
        log_info "Results saved to $results_file"
    else
        echo '{"error": "No results file generated", "tests": []}' > "$results_file"
        log_warn "No results file generated by mint"
    fi

    # Parse and display summary
    if command -v jq &> /dev/null && [[ -f "$results_file" ]]; then
        local passed failed na total
        passed=$(jq -s '[.[] | select(.status == "PASS")] | length' "$results_file" 2>/dev/null || echo "0")
        failed=$(jq -s '[.[] | select(.status == "FAIL")] | length' "$results_file" 2>/dev/null || echo "0")
        na=$(jq -s '[.[] | select(.status == "NA")] | length' "$results_file" 2>/dev/null || echo "0")
        total=$((passed + failed + na))

        echo ""
        log_info "============================================"
        log_info "S3 Compatibility Test Results"
        log_info "============================================"
        log_info "Total:   $total tests"
        log_success "Passed:  $passed"
        if [[ "$failed" -gt 0 ]]; then
            log_error "Failed:  $failed"
        else
            log_info "Failed:  $failed"
        fi
        log_info "N/A:     $na"

        if [[ $total -gt 0 ]]; then
            local compat_pct=$((passed * 100 / total))
            log_info "Compatibility: ${compat_pct}%"
        fi

        # Show failed tests
        if [[ "$failed" -gt 0 ]]; then
            echo ""
            log_warn "Failed tests:"
            jq -rs '.[] | select(.status == "FAIL") | "  [\(.function)] \(.name // "unknown")"' "$results_file" 2>/dev/null | head -20 || true
        fi
    fi

    # Cleanup
    rm -rf "$mint_log_dir" 2>/dev/null || true

    log_info ""
    log_info "Full log: $log_file"
    log_info "Results:  $results_file"

    return $exit_code
}

# Main
main() {
    local runtime
    runtime=$(get_runtime)

    log_info "Using container runtime: $runtime"

    mkdir -p "$REPORTS_DIR"

    if [[ "$BUILD_IMAGE" == "true" ]]; then
        build_image "$runtime"
    fi

    run_tests "$runtime"
}

main "$@"
