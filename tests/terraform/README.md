# OpenTofu/Terraform Integration Tests

This directory contains integration tests that verify Rucket's S3 API compatibility
with the Terraform AWS provider.

## Prerequisites

- [OpenTofu](https://opentofu.org/) or [Terraform](https://www.terraform.io/)
- Rucket built from source (`cargo build`)

## Running Tests

### Automated Testing

The simplest way to run tests is with the test runner script:

```bash
./run-tests.sh
```

This will:
1. Build Rucket (if not already built)
2. Start a Rucket server on port 9000
3. Initialize Terraform
4. Run `tofu plan` and `tofu apply`
5. Verify resources were created
6. Run `tofu destroy`
7. Clean up

### Options

```bash
# Skip starting the server (use if Rucket is already running)
./run-tests.sh --skip-server

# Keep Terraform state after tests (for debugging)
./run-tests.sh --keep-state

# Show detailed output
./run-tests.sh --verbose
```

### Manual Testing

For manual testing or debugging:

```bash
# Start Rucket with test credentials
RUCKET_ACCESS_KEY=test-access-key \
RUCKET_SECRET_KEY=test-secret-key \
cargo run -- --data-dir /tmp/rucket-test --port 9000

# In another terminal, run Terraform
cd tests/terraform
tofu init
tofu plan
tofu apply
tofu destroy
```

## Test Coverage

| Feature | Resource | Test File |
|---------|----------|-----------|
| Basic Bucket | `aws_s3_bucket` | `test_bucket_basic.tf` |
| Versioning | `aws_s3_bucket_versioning` | `test_versioning.tf` |
| CORS | `aws_s3_bucket_cors_configuration` | `test_cors.tf` |
| Bucket Policy | `aws_s3_bucket_policy` | `test_policy.tf` |
| Lifecycle | `aws_s3_bucket_lifecycle_configuration` | `test_lifecycle.tf` |
| Encryption | `aws_s3_bucket_server_side_encryption_configuration` | `test_encryption.tf` |
| Public Access Block | `aws_s3_bucket_public_access_block` | `test_public_access.tf` |
| Tagging | `aws_s3_bucket` (tags) | `test_tagging.tf` |
| Object Lock | `aws_s3_bucket_object_lock_configuration` | `test_object_lock.tf` |
| Objects | `aws_s3_object` | `test_objects.tf` |

## Provider Configuration

The tests use the AWS provider configured for Rucket:

```hcl
provider "aws" {
  access_key = "test-access-key"
  secret_key = "test-secret-key"
  region     = "us-east-1"

  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    s3 = "http://localhost:9000"
  }
}
```

## Troubleshooting

### "NoSuchBucket" errors

Make sure Rucket is running and accessible at `http://localhost:9000`.

### Provider initialization errors

Run `tofu init` to download the AWS provider.

### Permission denied

Check that Rucket has write access to its data directory.
