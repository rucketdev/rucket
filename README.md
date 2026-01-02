# Rucket

[![CI](https://github.com/rucketdev/rucket/actions/workflows/ci.yml/badge.svg)](https://github.com/rucketdev/rucket/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rucketdev/rucket/graph/badge.svg)](https://codecov.io/gh/rucketdev/rucket)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

S3-compatible object storage server written in Rust.

> [!CAUTION]
> **Not production-ready.** This project is under active development. APIs may change. Data durability is not guaranteed. Target: v1.0 for production use.

## Features

- S3 API compatibility (PUT, GET, DELETE, LIST, multipart uploads, versioning)
- Single-node deployment
- redb-backed metadata storage
- AWS Signature V4 authentication
- Streaming transfers
- Range requests

## Quick Start

```bash
# Install
cargo install rucket

# Run
rucket serve

# Or with config
rucket serve --config rucket.toml
```

**Configuration** (`rucket.toml`):

```toml
[server]
bind = "0.0.0.0:9000"

[storage]
data_dir = "/var/lib/rucket/data"

[auth]
access_key = "your-access-key"
secret_key = "your-secret-key"
```

**Usage**:

```bash
export AWS_ACCESS_KEY_ID=rucket
export AWS_SECRET_ACCESS_KEY=rucket123

aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket/
```

## S3 Compatibility

| Category | Operations | Status |
|----------|------------|--------|
| Buckets | Create, Delete, Head, List | ✅ |
| Objects | Put, Get, Delete, Head, Copy | ✅ |
| Listing | ListObjectsV1/V2, ListVersions | ✅ |
| Multipart | Create, Upload, Complete, Abort, List | ✅ |
| Versioning | Put/Get versions, Delete markers | ✅ |
| Metadata | User metadata, Content-Type, Cache-Control | ✅ |
| Conditional | If-Match, If-None-Match, If-Modified-Since | ✅ |
| Range | Byte range requests | ✅ |

See [Roadmap](docs/ROADMAP.md) for planned features.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   axum      │────▶│   Storage   │────▶│ Filesystem  │
│  (HTTP)     │     │   Backend   │     │   + redb    │
└─────────────┘     └─────────────┘     └─────────────┘
```

- **HTTP**: axum async runtime
- **Storage**: Local filesystem, UUID-based object storage
- **Metadata**: redb for transactional operations

## Development

```bash
git clone https://github.com/rucketdev/rucket.git
cd rucket
make test        # Run tests
make lint        # Format + clippy
make coverage    # Generate coverage report
```

## License

Apache 2.0. See [LICENSE](LICENSE).
