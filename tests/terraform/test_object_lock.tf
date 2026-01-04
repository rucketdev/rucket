# Object Lock Configuration Tests
#
# Tests: aws_s3_bucket with object lock and aws_s3_bucket_object_lock_configuration
# - Governance mode
# - Compliance mode (commented - requires special handling)
# - Default retention

resource "aws_s3_bucket" "object_lock" {
  bucket        = "${local.bucket_prefix}-objlock-${local.bucket_suffix}"
  force_destroy = true

  object_lock_enabled = true
}

resource "aws_s3_bucket_versioning" "object_lock" {
  bucket = aws_s3_bucket.object_lock.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_object_lock_configuration" "object_lock" {
  bucket = aws_s3_bucket.object_lock.id

  rule {
    default_retention {
      mode = "GOVERNANCE"
      days = 1
    }
  }

  depends_on = [aws_s3_bucket_versioning.object_lock]
}

output "object_lock_bucket_id" {
  description = "ID of the object lock bucket"
  value       = aws_s3_bucket.object_lock.id
}
