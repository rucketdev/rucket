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
| Server-Side Encryption | Partial | SSE-S3 implemented; SSE-C/SSE-KMS pending |
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
- GetBucketEncryption / PutBucketEncryption / DeleteBucketEncryption
- Default bucket encryption

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
- SSE-C (Customer-provided keys)
- SSE-KMS (AWS KMS keys)

**Rationale**: SSE-S3 is implemented; SSE-C and SSE-KMS require key management infrastructure.

### Access Control Lists
- Object ACLs
- Bucket ACLs
- Canned ACLs (public-read, private, etc.)
- Grant headers

**Rationale**: Single-user mode in Phase 1; bucket policies provide access control.

### Storage Classes
- Transition rules between storage classes
- Intelligent-Tiering

**Rationale**: Currently all data uses STANDARD storage class.

### Bucket Logging
- Access logging to target bucket

**Rationale**: Enterprise feature; planned for future phases.

### POST Object
- Browser-based uploads
- Form-based authentication

**Rationale**: Less common API; may implement in Phase 3.

### Other Missing Features
- Inventory
- Analytics
- Metrics
- Notifications (SNS/SQS/Lambda)
- Select Object Content
- Torrent

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
| Phase 5 | ACLs, POST Object, SSE-C/SSE-KMS | Future |
