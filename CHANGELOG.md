# Changelog

All notable changes to Rucket will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Features

- S3-compatible object storage server
- PUT, GET, DELETE, HEAD object operations
- Bucket create, delete, head, list operations
- ListObjectsV1/V2 with pagination and delimiter support
- Multipart uploads (create, upload part, complete, abort, list)
- Versioning support (put/get versions, delete markers)
- User metadata and system metadata (Content-Type, Cache-Control, etc.)
- Conditional requests (If-Match, If-None-Match, If-Modified-Since)
- Byte range requests
- AWS Signature V4 authentication
- redb-backed metadata storage
- Streaming transfers
- Ceph compatibility mode

### Testing

- 472 passing S3 compatibility tests
- 47% S3 API coverage

[Unreleased]: https://github.com/rucketdev/rucket/commits/main
