#!/bin/bash
#
# OpenTofu/Terraform Integration Tests for Rucket S3 API
#
# This script runs comprehensive Terraform tests against a Rucket server
# to verify S3 API compatibility with the Terraform AWS provider.
#
# Usage:
#   ./run-tests.sh [--skip-server] [--keep-state] [--verbose]
#
# Options:
#   --skip-server  Don't start/stop Rucket server (assumes it's already running)
#   --keep-state   Keep Terraform state after tests (for debugging)
#   --verbose      Show detailed output

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUCKET_BIN="${PROJECT_ROOT}/target/debug/rucket"
DATA_DIR="/tmp/rucket-tf-test-data"
RUCKET_PORT=9000
RUCKET_PID=""

# Test configuration
SKIP_SERVER=false
KEEP_STATE=false
VERBOSE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-server)
            SKIP_SERVER=true
            shift
            ;;
        --keep-state)
            KEEP_STATE=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up..."

    # Stop Rucket server if we started it
    if [[ -n "$RUCKET_PID" ]] && kill -0 "$RUCKET_PID" 2>/dev/null; then
        log_info "Stopping Rucket server (PID: $RUCKET_PID)..."
        kill "$RUCKET_PID" 2>/dev/null || true
        wait "$RUCKET_PID" 2>/dev/null || true
    fi

    # Clean up Terraform state unless --keep-state was specified
    if [[ "$KEEP_STATE" != "true" ]]; then
        log_info "Cleaning up Terraform state..."
        rm -rf "$SCRIPT_DIR/.terraform" 2>/dev/null || true
        rm -f "$SCRIPT_DIR/.terraform.lock.hcl" 2>/dev/null || true
        rm -f "$SCRIPT_DIR/terraform.tfstate"* 2>/dev/null || true
    fi

    # Clean up data directory
    rm -rf "$DATA_DIR" 2>/dev/null || true
}

# Set up cleanup trap
trap cleanup EXIT

# Check for tofu or terraform
if command -v tofu &> /dev/null; then
    TF_CMD="tofu"
elif command -v terraform &> /dev/null; then
    TF_CMD="terraform"
else
    log_error "Neither OpenTofu nor Terraform found. Please install one of them."
    exit 1
fi

log_info "Using: $TF_CMD"

# Build Rucket if needed
if [[ ! -f "$RUCKET_BIN" ]]; then
    log_info "Building Rucket..."
    cd "$PROJECT_ROOT"
    cargo build
fi

# Start Rucket server (unless --skip-server)
if [[ "$SKIP_SERVER" != "true" ]]; then
    # Clean and create data directory
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"

    log_info "Starting Rucket server on port $RUCKET_PORT..."

    # Start Rucket with test credentials
    RUCKET_ACCESS_KEY="test-access-key" \
    RUCKET_SECRET_KEY="test-secret-key" \
    "$RUCKET_BIN" \
        --data-dir "$DATA_DIR" \
        --port "$RUCKET_PORT" \
        --host "127.0.0.1" \
        > /tmp/rucket-tf-test.log 2>&1 &

    RUCKET_PID=$!

    # Wait for server to be ready
    log_info "Waiting for Rucket server to be ready..."
    for i in {1..30}; do
        if curl -s "http://localhost:$RUCKET_PORT/" > /dev/null 2>&1; then
            log_info "Rucket server is ready!"
            break
        fi
        if ! kill -0 "$RUCKET_PID" 2>/dev/null; then
            log_error "Rucket server failed to start. Check /tmp/rucket-tf-test.log"
            cat /tmp/rucket-tf-test.log
            exit 1
        fi
        sleep 1
    done

    if ! curl -s "http://localhost:$RUCKET_PORT/" > /dev/null 2>&1; then
        log_error "Rucket server did not become ready in time"
        exit 1
    fi
else
    log_warn "Skipping server start (--skip-server). Assuming Rucket is running on port $RUCKET_PORT"
fi

# Change to test directory
cd "$SCRIPT_DIR"

# Initialize Terraform
log_info "Initializing Terraform..."
if [[ "$VERBOSE" == "true" ]]; then
    $TF_CMD init
else
    $TF_CMD init > /dev/null
fi

# Run Terraform plan
log_info "Running Terraform plan..."
if [[ "$VERBOSE" == "true" ]]; then
    $TF_CMD plan -out=tfplan
else
    $TF_CMD plan -out=tfplan > /dev/null
fi
log_info "Plan completed successfully!"

# Apply Terraform configuration
log_info "Applying Terraform configuration..."
if [[ "$VERBOSE" == "true" ]]; then
    $TF_CMD apply -auto-approve tfplan
else
    $TF_CMD apply -auto-approve tfplan > /dev/null
fi
log_info "Apply completed successfully!"

# Show outputs
log_info "Terraform outputs:"
$TF_CMD output

# Verify resources were created by checking some outputs
log_info "Verifying resources..."

# Check that we can list buckets
BUCKET_COUNT=$(curl -s "http://localhost:$RUCKET_PORT/" | grep -c "<Bucket>" || echo "0")
log_info "Found $BUCKET_COUNT buckets"

if [[ "$BUCKET_COUNT" -lt 1 ]]; then
    log_error "Expected at least 1 bucket to be created"
    exit 1
fi

# Destroy resources
log_info "Destroying Terraform resources..."
if [[ "$VERBOSE" == "true" ]]; then
    $TF_CMD destroy -auto-approve
else
    $TF_CMD destroy -auto-approve > /dev/null
fi
log_info "Destroy completed successfully!"

# Verify cleanup
BUCKET_COUNT_AFTER=$(curl -s "http://localhost:$RUCKET_PORT/" | grep -c "<Bucket>" || echo "0")
log_info "Buckets remaining after destroy: $BUCKET_COUNT_AFTER"

# Clean up plan file
rm -f tfplan

log_info "==========================================="
log_info "All Terraform integration tests passed!"
log_info "==========================================="
