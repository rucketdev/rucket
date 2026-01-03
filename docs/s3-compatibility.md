# S3 Compatibility Status

This document describes Rucket's compatibility with the Amazon S3 API, including supported features, known limitations, and planned improvements.

## Test Results Summary

**Ceph s3-tests Pass Rate: 36%** (305/829 tests passing)

| Category | Status | Notes |
|----------|--------|-------|
| Basic Operations | Supported | PUT, GET, DELETE, HEAD, LIST |
| Bucket Operations | Supported | Create, delete, list, location |
| Object Versioning | Partial | Basic versioning works; some edge cases pending |
| Multipart Upload | Partial | Basic multipart works; versioning integration pending |
| Presigned URLs | Partial | Basic presigning works; some expiration edge cases |
| CORS | Partial | Configuration works; preflight responses pending |

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

### Checksums
- CRC32C (x-amz-checksum-crc32c)
- SHA256 (x-amz-checksum-sha256)

### Authentication
- AWS Signature Version 4 (header and query string)
- Presigned URLs

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

### Server-Side Encryption (105 failing tests)
- SSE-S3 (AES256)
- SSE-C (Customer-provided keys)
- SSE-KMS (AWS KMS keys)
- Bucket default encryption

**Rationale**: Encryption adds complexity; planned for Phase 3.

### Access Control Lists (51 failing tests)
- Object ACLs
- Bucket ACLs
- Canned ACLs (public-read, private, etc.)
- Grant headers

**Rationale**: Single-user mode in Phase 1; multi-user ACLs planned for Phase 3.

### Bucket Policies (32 failing tests)
- Policy-based access control
- Condition keys
- Principal specifications

**Rationale**: Requires multi-user support; planned for Phase 3.

### Object Lock (39 failing tests)
- Retention periods
- Legal holds
- Governance/Compliance modes

**Rationale**: Enterprise feature; planned for Phase 4.

### Lifecycle Rules (25 failing tests)
- Expiration rules
- Transition rules
- NoncurrentVersion actions

**Rationale**: Planned for Phase 2.

### Bucket Logging (32 failing tests)
- Access logging to target bucket

**Rationale**: Enterprise feature; planned for Phase 4.

### POST Object (21 failing tests)
- Browser-based uploads
- Form-based authentication

**Rationale**: Less common API; may implement in Phase 3.

### Other Missing Features
- Bucket Tagging
- Object Tagging
- Replication
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

| Phase | Features | Target |
|-------|----------|--------|
| Phase 1 | Core S3 operations, versioning, multipart | Current |
| Phase 2 | Lifecycle, improved multipart | Next |
| Phase 3 | Encryption, ACLs, policies, POST Object | Future |
| Phase 4 | Object Lock, logging, replication | Future |
