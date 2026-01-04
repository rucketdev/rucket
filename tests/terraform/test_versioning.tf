# Versioning Configuration Tests
#
# Tests: aws_s3_bucket_versioning resource
# - Enable versioning
# - Suspend versioning

resource "aws_s3_bucket" "versioning" {
  bucket        = "${local.bucket_prefix}-versioning-${local.bucket_suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.versioning.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Bucket with suspended versioning
resource "aws_s3_bucket" "versioning_suspended" {
  bucket        = "${local.bucket_prefix}-vers-suspended-${local.bucket_suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "versioning_suspended" {
  bucket = aws_s3_bucket.versioning_suspended.id

  versioning_configuration {
    status = "Suspended"
  }

  depends_on = [aws_s3_bucket.versioning_suspended]
}

output "versioning_bucket_id" {
  description = "ID of the versioned bucket"
  value       = aws_s3_bucket.versioning.id
}

output "versioning_status" {
  description = "Versioning status"
  value       = aws_s3_bucket_versioning.versioning.versioning_configuration[0].status
}
