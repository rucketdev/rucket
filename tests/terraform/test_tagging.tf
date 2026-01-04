# Bucket Tagging Tests
#
# Tests: aws_s3_bucket tags (inline) and bucket tagging resources
# - Add tags
# - Update tags
# - Multiple tags

resource "aws_s3_bucket" "tagged" {
  bucket        = "${local.bucket_prefix}-tagged-${local.bucket_suffix}"
  force_destroy = true

  tags = {
    Environment = "test"
    Project     = "rucket"
    Team        = "storage"
    CostCenter  = "12345"
  }
}

# Bucket with minimal tags
resource "aws_s3_bucket" "minimal_tags" {
  bucket        = "${local.bucket_prefix}-mintag-${local.bucket_suffix}"
  force_destroy = true

  tags = {
    Name = "MinimalTagsBucket"
  }
}

output "tagged_bucket_id" {
  description = "ID of the tagged bucket"
  value       = aws_s3_bucket.tagged.id
}

output "tagged_bucket_tags" {
  description = "Tags on the tagged bucket"
  value       = aws_s3_bucket.tagged.tags
}
