# Object Operations Tests
#
# Tests: aws_s3_object resource
# - Upload object
# - Object with metadata
# - Object with tags

resource "aws_s3_bucket" "objects" {
  bucket        = "${local.bucket_prefix}-objects-${local.bucket_suffix}"
  force_destroy = true
}

# Simple text object
resource "aws_s3_object" "simple" {
  bucket       = aws_s3_bucket.objects.id
  key          = "simple.txt"
  content      = "Hello, Rucket!"
  content_type = "text/plain"
}

# Object with metadata
resource "aws_s3_object" "with_metadata" {
  bucket       = aws_s3_bucket.objects.id
  key          = "data/metadata.json"
  content      = jsonencode({ message = "Hello", timestamp = timestamp() })
  content_type = "application/json"

  metadata = {
    "x-custom-header" = "custom-value"
    "environment"     = "test"
  }
}

# Object with tags
resource "aws_s3_object" "with_tags" {
  bucket       = aws_s3_bucket.objects.id
  key          = "tagged/file.txt"
  content      = "Tagged content"
  content_type = "text/plain"

  tags = {
    Environment = "test"
    Purpose     = "testing"
  }
}

# Binary-like object (base64 encoded)
resource "aws_s3_object" "binary" {
  bucket         = aws_s3_bucket.objects.id
  key            = "binary/data.bin"
  content_base64 = base64encode("Binary content simulation")
  content_type   = "application/octet-stream"
}

output "objects_bucket_id" {
  description = "ID of the objects bucket"
  value       = aws_s3_bucket.objects.id
}

output "simple_object_key" {
  description = "Key of the simple object"
  value       = aws_s3_object.simple.key
}

output "simple_object_etag" {
  description = "ETag of the simple object"
  value       = aws_s3_object.simple.etag
}
