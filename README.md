# Rucket

[![CI](https://github.com/rucketdev/rucket/actions/workflows/ci.yml/badge.svg)](https://github.com/rucketdev/rucket/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rucketdev/rucket/graph/badge.svg)](https://codecov.io/gh/rucketdev/rucket)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

> **Warning**: This project is experimental and not ready for production use. Production-ready release planned for version 1.x.

A high-performance, S3-compatible object storage server written in Rust.

## Features

- Full S3 API compatibility (PUT, GET, DELETE, LIST, multipart uploads)
- Single-node deployment (distributed mode coming soon)
- redb-backed metadata for reliability
- AWS Signature V4 authentication
- Streaming uploads and downloads
- Range request support

## Quick Start

### Installation

**From source:**

```bash
cargo install rucket
```

**From binary releases:**

Download from [GitHub Releases](https://github.com/rucketdev/rucket/releases)

### Running

```bash
# Start with defaults
rucket serve

# With custom config
rucket serve --config /path/to/rucket.toml
```

### Configuration

Create `rucket.toml`:

```toml
[server]
bind = "0.0.0.0:9000"

[storage]
data_dir = "/var/lib/rucket/data"

[auth]
access_key = "your-access-key"
secret_key = "your-secret-key"
```

### Usage with AWS CLI

```bash
# Configure AWS CLI
export AWS_ACCESS_KEY_ID=rucket
export AWS_SECRET_ACCESS_KEY=rucket123

# Create bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket

# Upload file
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/

# List objects
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket/

# Download file
aws --endpoint-url http://localhost:9000 s3 cp s3://my-bucket/file.txt downloaded.txt

# Delete file
aws --endpoint-url http://localhost:9000 s3 rm s3://my-bucket/file.txt
```

### Usage with MinIO Client

```bash
# Configure mc
mc alias set rucket http://localhost:9000 rucket rucket123

# Create bucket
mc mb rucket/my-bucket

# Upload file
mc cp file.txt rucket/my-bucket/

# List objects
mc ls rucket/my-bucket/
```

## S3 API Compatibility

### Supported Operations

| Operation | Status |
|-----------|--------|
| CreateBucket | ✅ |
| DeleteBucket | ✅ |
| HeadBucket | ✅ |
| ListBuckets | ✅ |
| PutObject | ✅ |
| GetObject | ✅ |
| DeleteObject | ✅ |
| HeadObject | ✅ |
| CopyObject | ✅ |
| ListObjectsV2 | ✅ |
| CreateMultipartUpload | ✅ |
| UploadPart | ✅ |
| CompleteMultipartUpload | ✅ |
| AbortMultipartUpload | ✅ |
| ListParts | ✅ |
| ListMultipartUploads | ✅ |

### Not Yet Implemented

- Object versioning
- Bucket policies and ACLs
- Pre-signed URLs
- Server-side encryption
- Lifecycle rules
- Replication

## Architecture

Rucket uses a simple but effective architecture:

- **HTTP Layer**: axum for high-performance async HTTP handling
- **Storage Layer**: Local filesystem with UUID-based object storage
- **Metadata Layer**: redb for atomic, transactional metadata operations
- **Authentication**: AWS Signature V4 with static credentials

```
data/
├── {bucket-name}/
│   ├── {uuid1}.dat          # Object data files
│   ├── {uuid2}.dat
│   └── ...
└── metadata.redb            # redb database
```

## Development

```bash
# Clone repository
git clone https://github.com/rucketdev/rucket.git
cd rucket

# Build
cargo build

# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run -- serve
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
