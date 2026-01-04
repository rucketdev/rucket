# Bucket Policy Tests
#
# Tests: aws_s3_bucket_policy resource
# - Attach policy
# - Complex policy with conditions

resource "aws_s3_bucket" "policy" {
  bucket        = "${local.bucket_prefix}-policy-${local.bucket_suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_policy" "policy" {
  bucket = aws_s3_bucket.policy.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "PublicReadGetObject"
        Effect    = "Allow"
        Principal = "*"
        Action    = "s3:GetObject"
        Resource  = "${aws_s3_bucket.policy.arn}/*"
      },
      {
        Sid       = "DenyDelete"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:DeleteObject"
        Resource  = "${aws_s3_bucket.policy.arn}/*"
        Condition = {
          StringNotLike = {
            "aws:PrincipalArn" = "arn:aws:iam::*:root"
          }
        }
      }
    ]
  })
}

output "policy_bucket_id" {
  description = "ID of the policy bucket"
  value       = aws_s3_bucket.policy.id
}
