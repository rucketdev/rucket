# S3 Compatibility Status

This document describes Rucket's compatibility with the Amazon S3 API, including supported features, known limitations, and planned improvements.

## Test Results Summary

| Category | Status | Notes |
|----------|--------|-------|
| Basic Operations | Supported | PUT, GET, DELETE, HEAD, LIST |
| Bucket Operations | Supported | Create, delete, list, location, policies |
| Object Versioning | Partial | Basic versioning works; some edge cases pending |
| Multipart Upload | Partial | Basic multipart works; versioning integration pending |
| Presigned URLs | Partial | Basic presigning works; some expiration edge cases |
| CORS | Partial | Configuration works; preflight responses pending |
| Object Lock | Supported | Retention (Governance/Compliance) and Legal Hold |
| Server-Side Encryption | Partial | SSE-S3 and SSE-C implemented; SSE-KMS pending |
| Bucket Policies | Supported | Policy CRUD and request-time evaluation |
| Object Tagging | Supported | Full tagging support including versioned objects |
| Bucket Tagging | Supported | Full bucket tagging support |
| Lifecycle Rules | Supported | Expiration and non-current version rules |
| Replication | Supported | Cross-region async replication |

## Fully Supported Features

### Bucket Operations
- CreateBucket
- DeleteBucket
- HeadBucket
- ListBuckets
- GetBucketLocation
- GetBucketVersioning / PutBucketVersioning

### Object Operations
- PutObject (including chunked transfer encoding)
- GetObject (including range requests)
- HeadObject
- DeleteObject
- DeleteObjects (batch delete)
- CopyObject (basic, same bucket or cross-bucket)
- ListObjects / ListObjectsV2
- ListObjectVersions

### Object Metadata
- User-defined metadata (x-amz-meta-*)
- Content-Type, Content-Encoding, Content-Disposition
- Cache-Control, Expires
- ETag (MD5 for simple uploads, multipart format for multipart)

### Object Tagging
- PutObjectTagging / GetObjectTagging / DeleteObjectTagging
- Tagging with versioned objects
- Tagging header on PutObject

### Object Lock
- PutObjectLockConfiguration / GetObjectLockConfiguration
- PutObjectRetention / GetObjectRetention (Governance and Compliance modes)
- PutObjectLegalHold / GetObjectLegalHold
- Retention enforcement on delete

### Server-Side Encryption
- SSE-S3 (AES-256-GCM encryption at rest)
- SSE-C (Customer-provided keys with AES-256-GCM)
- GetBucketEncryption / PutBucketEncryption / DeleteBucketEncryption
- Default bucket encryption

### Storage Classes
- STANDARD, REDUCED_REDUNDANCY, STANDARD_IA, ONEZONE_IA
- INTELLIGENT_TIERING, GLACIER, DEEP_ARCHIVE, GLACIER_IR
- Storage class on PutObject (x-amz-storage-class)
- Storage class preserved on CopyObject

### Bucket Logging
- PutBucketLogging / GetBucketLogging
- Access log delivery to target bucket
- Log prefix configuration

### POST Object (Browser Uploads)
- Multipart/form-data uploads
- POST policy validation (expiration, conditions)
- Success action redirect/status
- ${filename} variable substitution

### Checksums
- CRC32C (x-amz-checksum-crc32c)
- SHA256 (x-amz-checksum-sha256)

### Authentication
- AWS Signature Version 4 (header and query string)
- Presigned URLs

### Bucket Policies
- PutBucketPolicy / GetBucketPolicy / DeleteBucketPolicy
- Policy evaluation on object operations
- Principal, Action, Resource matching
- Condition keys (IpAddress, SecureTransport, StringEquals)

### Bucket Tagging
- PutBucketTagging / GetBucketTagging / DeleteBucketTagging

### Lifecycle Rules
- PutBucketLifecycleConfiguration / GetBucketLifecycleConfiguration / DeleteBucketLifecycleConfiguration
- Object expiration rules
- Non-current version expiration

### Replication
- PutBucketReplication / GetBucketReplication / DeleteBucketReplication
- Cross-region async replication
- Replication status tracking

## Partially Supported Features

### Object Versioning
**Status**: Core functionality works

**Supported**:
- Enable/suspend versioning
- Version-specific GET/DELETE/HEAD
- Delete markers
- ListObjectVersions with pagination

**Limitations**:
- Some concurrent multi-object delete edge cases
- Copy versioned objects (pending implementation)

### Multipart Upload
**Status**: Basic functionality works

**Supported**:
- InitiateMultipartUpload
- UploadPart / UploadPartCopy
- CompleteMultipartUpload
- AbortMultipartUpload
- ListMultipartUploads
- ListParts

**Limitations**:
- Multipart with versioning has edge cases
- Part size validation differs slightly from AWS

### CORS
**Status**: Configuration works

**Supported**:
- PutBucketCors
- GetBucketCors
- DeleteBucketCors

**Limitations**:
- Preflight responses may differ from AWS
- Origin wildcard matching edge cases

## Not Implemented Features

### Server-Side Encryption (Partial)
- SSE-KMS (AWS KMS keys)

**Rationale**: Requires KMS infrastructure integration.

### Access Control Lists
- Object ACLs
- Bucket ACLs
- Canned ACLs (public-read, private, etc.)
- Grant headers

**Rationale**: Bucket policies provide access control; ACLs planned for Phase 5.

### Storage Class Transitions
- Lifecycle transition rules between storage classes
- Automatic Intelligent-Tiering

**Rationale**: Storage class headers are supported; automatic transitions require scheduler.

### Other Missing Features
- Inventory
- Analytics
- Metrics
- Notifications (SNS/SQS/Lambda)
- Select Object Content
- Torrent
- STS (Security Token Service)
- IAM (Identity and Access Management)

## Known Behavioral Differences

### Version ID Format
- AWS uses opaque strings (often base64-encoded)
- Rucket uses UUID v4 format

### Error Response Format
- Error XML structure matches AWS
- Some error codes may differ for edge cases

### Eventual Consistency
- Rucket provides strong consistency (unlike historical S3)
- No read-after-write delay

### Request ID
- Generated per-request
- Format differs from AWS

## Testing

### Running Ceph s3-tests

```bash
# Start Rucket
./target/release/rucket serve --config rucket.toml

# Run tests
./scripts/s3-compat/ceph-runner.sh --core
```

### Running Rucket Integration Tests

```bash
cargo test --package rucket --test s3_compat
```

## Compatibility Roadmap

| Phase | Features | Status |
|-------|----------|--------|
| Phase 1 | Core S3 operations, versioning, multipart | Complete |
| Phase 2.1 | Object Lock (retention, legal hold) | Complete |
| Phase 2.2 | Server-Side Encryption (SSE-S3) | Complete |
| Phase 2.3 | Bucket Policies (CRUD + evaluation) | Complete |
| Phase 3 | Bucket tagging, lifecycle rules | Complete |
| Phase 4 | Cross-region replication | Complete |
| Phase 4.1 | Storage Classes | Complete |
| Phase 4.2 | Anonymous Access | Complete |
| Phase 4.3 | Website Hosting | Complete |
| Phase 4.4 | Bucket Logging | Complete |
| Phase 4.5 | SSE-C (Customer-provided keys) | Complete |
| Phase 4.6 | POST Object (Browser uploads) | Complete |
| Phase 5 | ACLs, SSE-KMS | Future |
