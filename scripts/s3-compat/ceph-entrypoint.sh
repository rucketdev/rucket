#!/bin/bash
# Ceph S3-Tests Entrypoint
#
# This script runs ceph/s3-tests with the provided configuration.

set -e

S3TESTS_DIR="/opt/s3-tests"
CONFIG_FILE="${S3TEST_CONF:-/config/s3tests.conf}"
RESULTS_DIR="${RESULTS_DIR:-/results}"

usage() {
    cat << EOFUSAGE
Ceph S3 Compatibility Tests for Rucket

Usage: docker run rucket/s3-tests [OPTIONS]

Options:
  --all             Run all tests
  --core            Run core S3 functional tests (default)
  --boto3           Run boto3-based tests
  --test PATTERN    Run specific test (e.g., test_bucket_list_empty)
  --help            Show this help message

Environment Variables:
  S3TEST_CONF       Path to s3tests.conf (default: /config/s3tests.conf)
  RESULTS_DIR       Directory for test results (default: /results)
EOFUSAGE
    exit 0
}

run_tests() {
    local test_path="$1"
    local json_output="$RESULTS_DIR/ceph-results.json"
    
    cd "$S3TESTS_DIR"
    
    echo "Running s3-tests..."
    echo "  Config: $CONFIG_FILE"
    echo "  Tests: $test_path"
    echo ""
    
    # Run pytest with JSON output
    if ! S3TEST_CONF="$CONFIG_FILE" python -m pytest \
        "$test_path" \
        --json-report \
        --json-report-file="$json_output" \
        --tb=short \
        -v \
        2>&1; then
        echo ""
        echo "Some tests failed. See results in $json_output"
    fi
    
    # Generate summary
    if [ -f "$json_output" ]; then
        echo ""
        echo "============================================"
        echo "Test Results Summary"
        echo "============================================"
        python3 << PYSUM
import json
import sys
try:
    with open("$json_output") as f:
        data = json.load(f)
    summary = data.get("summary", {})
    passed = summary.get("passed", 0)
    failed = summary.get("failed", 0)
    error = summary.get("error", 0)
    skipped = summary.get("skipped", 0)
    total = summary.get("total", passed + failed + error + skipped)
    
    print(f"Total:   {total}")
    print(f"Passed:  {passed}")
    print(f"Failed:  {failed}")
    print(f"Error:   {error}")
    print(f"Skipped: {skipped}")
    if total > 0:
        pct = passed * 100 // total
        print(f"Pass Rate: {pct}%")
except Exception as e:
    print(f"Error parsing results: {e}", file=sys.stderr)
PYSUM
    fi
}

# Parse arguments
TEST_MODE="core"
SPECIFIC_TEST=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --all)
            TEST_MODE="all"
            shift
            ;;
        --core)
            TEST_MODE="core"
            shift
            ;;
        --boto3)
            TEST_MODE="boto3"
            shift
            ;;
        --test)
            TEST_MODE="specific"
            SPECIFIC_TEST="$2"
            shift 2
            ;;
        --help|-h)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Check config file
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file not found: $CONFIG_FILE"
    echo "Mount your config file: -v /path/to/s3tests.conf:/config/s3tests.conf"
    exit 1
fi

# Determine test path
case "$TEST_MODE" in
    all)
        TEST_PATH="s3tests/functional"
        ;;
    core)
        TEST_PATH="s3tests/functional/test_s3.py"
        ;;
    boto3)
        TEST_PATH="s3tests/functional"
        ;;
    specific)
        TEST_PATH="s3tests/functional/test_s3.py::$SPECIFIC_TEST"
        ;;
esac

run_tests "$TEST_PATH"
