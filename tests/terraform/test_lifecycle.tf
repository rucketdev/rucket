# Lifecycle Configuration Tests
#
# Tests: aws_s3_bucket_lifecycle_configuration resource
# - Expiration rules
# - Noncurrent version expiration
# - Abort incomplete multipart upload
# - Prefix filters

resource "aws_s3_bucket" "lifecycle" {
  bucket        = "${local.bucket_prefix}-lifecycle-${local.bucket_suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "lifecycle" {
  bucket = aws_s3_bucket.lifecycle.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lifecycle" {
  bucket = aws_s3_bucket.lifecycle.id

  # Rule to expire objects after 30 days
  rule {
    id     = "expire-old-objects"
    status = "Enabled"

    expiration {
      days = 30
    }
  }

  # Rule to clean up noncurrent versions
  rule {
    id     = "cleanup-noncurrent"
    status = "Enabled"

    filter {
      prefix = "logs/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  # Rule to abort incomplete multipart uploads
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.lifecycle]
}

output "lifecycle_bucket_id" {
  description = "ID of the lifecycle bucket"
  value       = aws_s3_bucket.lifecycle.id
}
