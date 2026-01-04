# Server-Side Encryption Configuration Tests
#
# Tests: aws_s3_bucket_server_side_encryption_configuration resource
# - AES256 encryption
# - Bucket key enabled

resource "aws_s3_bucket" "encryption" {
  bucket        = "${local.bucket_prefix}-encryption-${local.bucket_suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "encryption" {
  bucket = aws_s3_bucket.encryption.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Test bucket with encryption disabled (delete encryption config)
resource "aws_s3_bucket" "no_encryption" {
  bucket        = "${local.bucket_prefix}-no-encrypt-${local.bucket_suffix}"
  force_destroy = true
}

output "encryption_bucket_id" {
  description = "ID of the encrypted bucket"
  value       = aws_s3_bucket.encryption.id
}

output "no_encryption_bucket_id" {
  description = "ID of the non-encrypted bucket"
  value       = aws_s3_bucket.no_encryption.id
}
