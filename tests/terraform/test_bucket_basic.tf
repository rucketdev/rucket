# Basic Bucket Operations Tests
#
# Tests: aws_s3_bucket resource
# - Create bucket
# - Force destroy with objects

resource "aws_s3_bucket" "basic" {
  bucket        = "${local.bucket_prefix}-basic-${local.bucket_suffix}"
  force_destroy = true

  tags = {
    Name        = "BasicTestBucket"
    Environment = "test"
  }
}

# Verify bucket creation worked
output "basic_bucket_id" {
  description = "ID of the basic test bucket"
  value       = aws_s3_bucket.basic.id
}

output "basic_bucket_arn" {
  description = "ARN of the basic test bucket"
  value       = aws_s3_bucket.basic.arn
}

output "basic_bucket_region" {
  description = "Region of the basic test bucket"
  value       = aws_s3_bucket.basic.region
}
