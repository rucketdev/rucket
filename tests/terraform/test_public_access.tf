# Public Access Block Configuration Tests
#
# Tests: aws_s3_bucket_public_access_block resource
# - Block all public access
# - Partial blocking
# - Remove block

resource "aws_s3_bucket" "public_access_blocked" {
  bucket        = "${local.bucket_prefix}-blocked-${local.bucket_suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "blocked" {
  bucket = aws_s3_bucket.public_access_blocked.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Bucket with partial public access blocking
resource "aws_s3_bucket" "partial_blocked" {
  bucket        = "${local.bucket_prefix}-partial-${local.bucket_suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "partial" {
  bucket = aws_s3_bucket.partial_blocked.id

  block_public_acls       = true
  block_public_policy     = false
  ignore_public_acls      = true
  restrict_public_buckets = false
}

output "blocked_bucket_id" {
  description = "ID of the fully blocked bucket"
  value       = aws_s3_bucket.public_access_blocked.id
}

output "partial_blocked_bucket_id" {
  description = "ID of the partially blocked bucket"
  value       = aws_s3_bucket.partial_blocked.id
}
