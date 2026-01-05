# Rucket

[![CI](https://github.com/rucketdev/rucket/actions/workflows/ci.yml/badge.svg)](https://github.com/rucketdev/rucket/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rucketdev/rucket/graph/badge.svg)](https://codecov.io/gh/rucketdev/rucket)
[![License](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](LICENSE)

S3-compatible object storage. Community-governed, feature-complete, written in Rust.

> [!CAUTION]
> **Not production-ready.** Under active development. Target: v1.0 for production use.

## Goals

- **Open Source** — AGPL v3, foundation governance, no single-vendor control
- **S3 Compatible** — Drop-in replacement, not "S3-ish"
- **Performant** — Competitive with MinIO on single-node workloads
- **Durable** — Write-ahead logging, crash recovery, no silent data loss
- **Distributed** — Raft-based clustering with automatic rebalancing

See [VISION.md](docs/VISION.md) for details.

## Quick Start

```bash
# Docker
docker run -p 9000:9000 -v rucket-data:/data ghcr.io/rucketdev/rucket:latest

# Binary
curl -LO https://github.com/rucketdev/rucket/releases/latest/download/rucket-linux-amd64.tar.gz
tar -xzf rucket-linux-amd64.tar.gz && ./rucket serve

# Source
cargo install rucket && rucket serve
```

Use with AWS CLI:
```bash
export AWS_ACCESS_KEY_ID=rucket
export AWS_SECRET_ACCESS_KEY=rucket123

aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/
```

## Configuration

```toml
[server]
bind = "0.0.0.0:9000"

[storage]
data_dir = "/var/lib/rucket/data"

[auth]
access_key = "your-access-key"
secret_key = "your-secret-key"
```

See [configuration.md](docs/configuration.md) for all options.

## S3 Compatibility

| Feature | Status |
|---------|--------|
| Bucket/Object CRUD | Done |
| Multipart uploads | Done |
| Versioning | Done |
| Tagging | Done |
| Checksums | Done |
| Presigned URLs | Done |
| CORS | Done |
| Bucket policies | Done |
| Object Lock | Done |
| SSE-S3 encryption | Done |
| Lifecycle policies | Done |
| Replication | Done |

## License

AGPL-3.0. See [LICENSE](LICENSE).
