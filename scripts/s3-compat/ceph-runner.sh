#!/usr/bin/env bash

# Ceph S3 Compatibility Test Runner
#
# This script runs ceph/s3-tests against Rucket.
# It uses a Docker container with the s3-tests suite.
#
# Usage: ./ceph-runner.sh [OPTIONS]

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
CEPH_IMAGE="${CEPH_IMAGE:-rucket/s3-tests:latest}"
TIMEOUT_SECS="${TIMEOUT_SECS:-1800}"
TEST_MODE="core"

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
Ceph S3 Compatibility Test Runner

Usage: $0 [OPTIONS]

Options:
  --build-image       Build test image from Dockerfile.ceph
  --image NAME        Docker image to use (default: $CEPH_IMAGE)
  --timeout SECS      Timeout in seconds (default: $TIMEOUT_SECS)
  --all               Run all tests
  --core              Run core S3 tests (default)
  -h, --help          Show this help message

Environment Variables:
  RUCKET_ENDPOINT     Rucket S3 endpoint (default: $RUCKET_ENDPOINT)
  RUCKET_ACCESS_KEY   S3 access key (default: $RUCKET_ACCESS_KEY)
  RUCKET_SECRET_KEY   S3 secret key
  CEPH_IMAGE          Docker image to use
  TIMEOUT_SECS        Timeout in seconds

Examples:
  $0                          # Run core tests
  $0 --build-image            # Build image from source and run
  $0 --all                    # Run all tests
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
            CEPH_IMAGE="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT_SECS="$2"
            shift 2
            ;;
        --all)
            TEST_MODE="all"
            shift
            ;;
        --core)
            TEST_MODE="core"
            shift
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

# Generate s3tests.conf for Rucket
generate_config() {
    local config_file="$1"
    local host port
    
    # Parse endpoint
    if [[ "$RUCKET_ENDPOINT" =~ ^https?://([^:]+):([0-9]+) ]]; then
        host="${BASH_REMATCH[1]}"
        port="${BASH_REMATCH[2]}"
    elif [[ "$RUCKET_ENDPOINT" =~ ^https?://([^/]+) ]]; then
        host="${BASH_REMATCH[1]}"
        port="9000"
    else
        host="127.0.0.1"
        port="9000"
    fi
    
    cat > "$config_file" << EOCONF
[DEFAULT]
host = $host
port = $port
is_secure = False
ssl_verify = False

[fixtures]
bucket prefix = rucket-s3-test-{random}-

[s3 main]
display_name = Rucket Test User
user_id = testuser
email = test@rucket.dev
access_key = $RUCKET_ACCESS_KEY
secret_key = $RUCKET_SECRET_KEY
api_name = default

[s3 alt]
display_name = Rucket Alt User  
user_id = altuser
email = alt@rucket.dev
access_key = $RUCKET_ACCESS_KEY
secret_key = $RUCKET_SECRET_KEY
EOCONF
    
    log_info "Generated config: $config_file"
}

# Build test image
build_image() {
    local runtime="$1"
    local dockerfile="$SCRIPT_DIR/Dockerfile.ceph"
    
    if [[ ! -f "$dockerfile" ]]; then
        log_error "Dockerfile not found: $dockerfile"
        exit 1
    fi
    
    log_info "Building ceph s3-tests image..."
    log_info "This may take a few minutes on first build."
    
    $runtime build -f "$dockerfile" -t rucket/s3-tests:latest "$PROJECT_ROOT"
    
    CEPH_IMAGE="rucket/s3-tests:latest"
    log_success "Built image: $CEPH_IMAGE"
}

# Run tests
run_tests() {
    local runtime="$1"
    local config_dir
    local results_dir="$REPORTS_DIR"
    
    config_dir=$(mktemp -d)
    generate_config "$config_dir/s3tests.conf"
    
    mkdir -p "$results_dir"
    
    log_info "Running Ceph S3 compatibility tests..."
    log_info "  Image:    $CEPH_IMAGE"
    log_info "  Mode:     $TEST_MODE"
    log_info "  Timeout:  ${TIMEOUT_SECS}s"
    
    local exit_code=0
    if timeout "$TIMEOUT_SECS" $runtime run --rm --network=host \
        -v "$config_dir:/config:ro" \
        -v "$results_dir:/results" \
        -e "S3TEST_CONF=/config/s3tests.conf" \
        "$CEPH_IMAGE" "--$TEST_MODE" 2>&1 | tee "$results_dir/ceph-tests.log"; then
        log_success "Ceph s3-tests completed"
    else
        exit_code=$?
        if [[ $exit_code -eq 124 ]]; then
            log_warn "Tests timed out after $TIMEOUT_SECS seconds"
        else
            log_warn "Tests completed with exit code: $exit_code"
        fi
    fi
    
    # Cleanup
    rm -rf "$config_dir" 2>/dev/null || true
    
    log_info ""
    log_info "Results: $results_dir/ceph-results.json"
    log_info "Log:     $results_dir/ceph-tests.log"
    
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
    
    # Check if image exists
    if ! $runtime image inspect "$CEPH_IMAGE" &> /dev/null; then
        log_warn "Image $CEPH_IMAGE not found. Building..."
        build_image "$runtime"
    fi
    
    run_tests "$runtime"
}

main "$@"
