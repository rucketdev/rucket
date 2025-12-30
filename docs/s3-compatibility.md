# S3 API Compatibility

This document tracks Rucket's compatibility with the Amazon S3 API.

## Bucket Operations

| Operation | Status | Notes |
|-----------|--------|-------|
| CreateBucket | Supported | |
| DeleteBucket | Supported | Must be empty |
| HeadBucket | Supported | |
| ListBuckets | Supported | |
| GetBucketLocation | Not implemented | |
| GetBucketVersioning | Not implemented | |
| PutBucketVersioning | Not implemented | |

## Object Operations

| Operation | Status | Notes |
|-----------|--------|-------|
| PutObject | Supported | |
| GetObject | Supported | Range requests supported |
| DeleteObject | Supported | |
| HeadObject | Supported | |
| CopyObject | Supported | |
| ListObjectsV2 | Supported | Pagination supported |
| ListObjects (v1) | Not implemented | Use v2 |
| DeleteObjects | Not implemented | Batch delete |

## Multipart Upload

| Operation | Status | Notes |
|-----------|--------|-------|
| CreateMultipartUpload | Supported | |
| UploadPart | Supported | |
| CompleteMultipartUpload | Supported | |
| AbortMultipartUpload | Supported | |
| ListParts | Supported | |
| ListMultipartUploads | Supported | |
| UploadPartCopy | Not implemented | |

## Authentication

| Method | Status | Notes |
|--------|--------|-------|
| AWS Signature V4 | Supported | |
| AWS Signature V2 | Not implemented | Deprecated |
| Pre-signed URLs | Not implemented | Planned |

## Not Planned for MVP

- Object versioning
- Bucket policies
- ACLs
- Lifecycle rules
- Replication
- Server-side encryption
- Object tagging
- Object lock
- S3 Select
