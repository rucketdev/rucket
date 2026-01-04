# Terraform AWS Provider Configuration for Rucket S3-compatible storage
#
# This configuration allows OpenTofu/Terraform to work with Rucket's S3 API.
# All provider settings are configured to bypass AWS-specific features
# that are not applicable to a local S3-compatible storage backend.

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  # Test credentials (Rucket uses these for authentication)
  access_key = "test-access-key"
  secret_key = "test-secret-key"
  region     = "us-east-1"

  # Skip AWS-specific validation that doesn't apply to Rucket
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  # Use path-style addressing (required for Rucket)
  s3_use_path_style = true

  # Point to local Rucket server
  endpoints {
    s3 = "http://localhost:9000"
  }
}

# Random suffix for unique bucket names across test runs
resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  # Common bucket name prefix for all tests
  bucket_prefix = "rucket-tf-test"
  bucket_suffix = random_id.suffix.hex
}
