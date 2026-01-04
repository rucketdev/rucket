# CORS Configuration Tests
#
# Tests: aws_s3_bucket_cors_configuration resource
# - Add CORS rules
# - Multiple rules
# - All CORS rule options

resource "aws_s3_bucket" "cors" {
  bucket        = "${local.bucket_prefix}-cors-${local.bucket_suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_cors_configuration" "cors" {
  bucket = aws_s3_bucket.cors.id

  cors_rule {
    id              = "AllowAll"
    allowed_headers = ["*"]
    allowed_methods = ["GET", "PUT", "POST", "DELETE", "HEAD"]
    allowed_origins = ["*"]
    expose_headers  = ["ETag", "x-amz-request-id"]
    max_age_seconds = 3600
  }

  cors_rule {
    id              = "SpecificOrigin"
    allowed_headers = ["Authorization", "Content-Type"]
    allowed_methods = ["GET"]
    allowed_origins = ["https://example.com", "https://www.example.com"]
    max_age_seconds = 300
  }
}

output "cors_bucket_id" {
  description = "ID of the CORS bucket"
  value       = aws_s3_bucket.cors.id
}
