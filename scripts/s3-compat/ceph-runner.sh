#!/usr/bin/env bash
# Copyright 2026 Rucket Dev
# SPDX-License-Identifier: Apache-2.0

# Ceph S3-Tests Runner
# This script sets up and runs ceph/s3-tests against Rucket

set -euo pipefail

CEPH_REPO="https://github.com/ceph/s3-tests.git"
CEPH_BRANCH="master"

ceph_setup() {
    local vendor_dir="$1"
    local ceph_dir="$vendor_dir/s3-tests"

    if [[ -d "$ceph_dir" ]]; then
        log_info "ceph/s3-tests already cloned, updating..."
        cd "$ceph_dir"
        git fetch origin
        git reset --hard "origin/$CEPH_BRANCH"
    else
        log_info "Cloning ceph/s3-tests..."
        git clone --depth 1 --branch "$CEPH_BRANCH" "$CEPH_REPO" "$ceph_dir"
    fi

    cd "$ceph_dir"

    # Create virtual environment if it doesn't exist
    if [[ ! -d "venv" ]]; then
        log_info "Creating Python virtual environment..."
        python3 -m venv venv
    fi

    log_info "Installing dependencies..."
    source venv/bin/activate
    pip install --quiet --upgrade pip
    pip install --quiet tox boto3 nose

    deactivate
}

ceph_generate_config() {
    local endpoint="$1"
    local access_key="$2"
    local secret_key="$3"
    local config_file="$4"

    # Parse endpoint to extract host and port
    local host port is_secure
    if [[ "$endpoint" =~ ^https:// ]]; then
        is_secure="True"
        endpoint="${endpoint#https://}"
    else
        is_secure="False"
        endpoint="${endpoint#http://}"
    fi

    host="${endpoint%%:*}"
    port="${endpoint##*:}"
    port="${port%%/*}"

    # Default port if not specified
    if [[ "$host" == "$port" ]]; then
        if [[ "$is_secure" == "True" ]]; then
            port="443"
        else
            port="80"
        fi
    fi

    cat > "$config_file" << EOF
[DEFAULT]
host = $host
port = $port
is_secure = $is_secure
ssl_verify = False

[fixtures]
bucket prefix = rucket-test-{random}-

[s3 main]
display_name = Main User
user_id = testuser
email = testuser@example.com
access_key = $access_key
secret_key = $secret_key
api_name = default

[s3 alt]
display_name = Alt User
user_id = altuser
email = altuser@example.com
access_key = ${access_key}_alt
secret_key = ${secret_key}_alt
api_name = default

[s3 tenant]
display_name = Tenant User
user_id = tenantuser
email = tenantuser@example.com
access_key = ${access_key}_tenant
secret_key = ${secret_key}_tenant
api_name = default
EOF

    log_info "Generated config at $config_file"
}

ceph_run() {
    local endpoint="$1"
    local access_key="$2"
    local secret_key="$3"
    local reports_dir="$4"

    local vendor_dir
    vendor_dir="$(dirname "$(dirname "$reports_dir")")/.s3-compat-tests"
    local ceph_dir="$vendor_dir/s3-tests"
    local config_file="$ceph_dir/s3tests.conf"
    local results_file="$reports_dir/ceph-results.json"
    local log_file="$reports_dir/ceph-tests.log"

    cd "$ceph_dir"

    # Generate config
    ceph_generate_config "$endpoint" "$access_key" "$secret_key" "$config_file"

    source venv/bin/activate

    log_info "Running ceph/s3-tests (this may take a while)..."

    # Run tests with pytest and generate JSON report
    # We run a subset of tests that are most likely to pass on S3-compatible implementations
    # Exclude tests that require Ceph-specific features
    local test_args=(
        "-v"
        "--tb=short"
        "-x"  # Stop on first failure for faster feedback
        "-m" "not fails_on_rgw and not fails_on_aws"
        "s3tests_boto3/functional/test_s3.py"
    )

    # Create a Python script to run tests and capture results
    cat > run_tests.py << 'PYTEST_RUNNER'
import subprocess
import json
import sys
import os

def run_tests():
    result = {
        "passed": 0,
        "failed": 0,
        "skipped": 0,
        "errors": 0,
        "tests": []
    }

    env = os.environ.copy()
    env["S3TEST_CONF"] = "s3tests.conf"

    # Run pytest with JSON output
    proc = subprocess.run(
        [
            sys.executable, "-m", "pytest",
            "-v", "--tb=short",
            "-m", "not fails_on_rgw and not fails_on_aws and not lifecycle",
            "--ignore=s3tests_boto3/functional/test_iam.py",
            "--ignore=s3tests_boto3/functional/test_sts.py",
            "s3tests_boto3/functional/test_s3.py",
            "-q", "--no-header",
        ],
        capture_output=True,
        text=True,
        env=env,
        timeout=1800  # 30 minute timeout
    )

    # Parse output
    output = proc.stdout + proc.stderr
    lines = output.split('\n')

    for line in lines:
        if ' PASSED' in line:
            result["passed"] += 1
            result["tests"].append({"name": line.split()[0], "status": "PASS"})
        elif ' FAILED' in line:
            result["failed"] += 1
            result["tests"].append({"name": line.split()[0], "status": "FAIL"})
        elif ' SKIPPED' in line or ' skipped' in line:
            result["skipped"] += 1
        elif ' ERROR' in line:
            result["errors"] += 1
            result["tests"].append({"name": line.split()[0], "status": "ERROR"})

    result["returncode"] = proc.returncode
    result["output"] = output[-5000:]  # Last 5000 chars of output

    return result

if __name__ == "__main__":
    try:
        results = run_tests()
    except subprocess.TimeoutExpired:
        results = {"error": "Tests timed out after 30 minutes", "passed": 0, "failed": 0, "skipped": 0}
    except Exception as e:
        results = {"error": str(e), "passed": 0, "failed": 0, "skipped": 0}

    print(json.dumps(results, indent=2))
PYTEST_RUNNER

    # Install pytest if not present
    pip install --quiet pytest pytest-json-report 2>/dev/null || true

    # Run the test script
    if python3 run_tests.py > "$results_file" 2>"$log_file"; then
        log_success "ceph/s3-tests completed"
    else
        log_warn "ceph/s3-tests completed with some failures"
    fi

    # Show summary
    if command -v jq &> /dev/null && [[ -f "$results_file" ]]; then
        local passed failed skipped
        passed=$(jq -r '.passed // 0' "$results_file")
        failed=$(jq -r '.failed // 0' "$results_file")
        skipped=$(jq -r '.skipped // 0' "$results_file")
        log_info "Results: $passed passed, $failed failed, $skipped skipped"
    fi

    deactivate

    log_info "Full results saved to $results_file"
    log_info "Test log saved to $log_file"
}
