# Rucket

[![CI](https://github.com/rucketdev/rucket/actions/workflows/ci.yml/badge.svg)](https://github.com/rucketdev/rucket/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rucketdev/rucket/graph/badge.svg)](https://codecov.io/gh/rucketdev/rucket)
[![License](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](LICENSE)

S3-compatible object storage server written in Rust.

> [!CAUTION]
> **Not production-ready.** This project is under active development. APIs may change. Data durability is not guaranteed. Target: v1.0 for production use.

## Features

- **S3 API compatibility** - PUT, GET, DELETE, LIST, multipart uploads
- **Object versioning** - Full versioning with delete markers
- **Object tagging** - Tag objects and buckets
- **Checksums** - CRC32, CRC32C, SHA-1, SHA-256 verification
- **Conditional requests** - If-Match, If-None-Match, If-Modified-Since
- **Range requests** - Byte-range retrieval
- **CORS support** - Cross-origin resource sharing
- **TLS/HTTPS** - Native TLS support
- **Prometheus metrics** - Built-in metrics endpoint
- **Crash recovery** - Write-ahead log with configurable durability
- **AWS Signature V4** - Standard S3 authentication

## Installation

**Binary releases**:
```bash
curl -LO https://github.com/rucketdev/rucket/releases/download/v0.1.1/rucket-v0.1.1-linux-amd64.tar.gz
tar -xzf rucket-v0.1.1-linux-amd64.tar.gz
./rucket serve
```

**Docker**:
```bash
docker run -p 9000:9000 -v rucket-data:/data ghcr.io/rucketdev/rucket:latest
```

**From source**:
```bash
cargo install rucket
rucket serve
```

## Quick Start

```bash
rucket serve
```

Use with AWS CLI:
```bash
export AWS_ACCESS_KEY_ID=rucket
export AWS_SECRET_ACCESS_KEY=rucket123

aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket/
```

## Configuration

Create `rucket.toml`:

```toml
[server]
bind = "0.0.0.0:9000"
# tls_cert = "/path/to/cert.pem"
# tls_key = "/path/to/key.pem"

[storage]
data_dir = "/var/lib/rucket/data"
# durability = "balanced"  # performance | balanced | durable

[auth]
access_key = "your-access-key"
secret_key = "your-secret-key"

[metrics]
enabled = true
port = 9001
```

## S3 Compatibility

| Feature | Status |
|---------|--------|
| Bucket CRUD | ✅ |
| Object CRUD | ✅ |
| Multipart uploads | ✅ |
| Object versioning | ✅ |
| Object tagging | ✅ |
| Bucket tagging | ✅ |
| CORS | ✅ |
| Checksums | ✅ |
| Range requests | ✅ |
| Conditional requests | ✅ |
| Presigned URLs | ✅ |
| Bucket policies | ⚠️ Stored, not enforced |
| ACLs | ❌ |
| Encryption (SSE) | ❌ |
| Object Lock | ❌ |
| Lifecycle rules | ❌ |
| Replication | ❌ |

Test suite: 548/981 passing (55.8%). See [ROADMAP.md](docs/ROADMAP.md) for planned features.

## License

AGPL-3.0. See [LICENSE](LICENSE).
