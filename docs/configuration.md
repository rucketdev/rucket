# Configuration Reference

Rucket is configured via a TOML file. By default, it looks for `rucket.toml` in the current directory or `/etc/rucket/rucket.toml`.

## Server Section

```toml
[server]
bind = "0.0.0.0:9000"
```

| Option | Default | Description |
|--------|---------|-------------|
| `bind` | `127.0.0.1:9000` | Address and port to listen on |

## Storage Section

```toml
[storage]
data_dir = "/var/lib/rucket/data"
```

| Option | Default | Description |
|--------|---------|-------------|
| `data_dir` | `./data` | Directory for object data and metadata |

The `temp_dir` defaults to `{data_dir}/.tmp`.

## Auth Section

```toml
[auth]
access_key = "your-access-key"
secret_key = "your-secret-key"
```

| Option | Default | Description |
|--------|---------|-------------|
| `access_key` | `rucket` | AWS-compatible access key ID |
| `secret_key` | `rucket123` | AWS-compatible secret access key |

## Bucket Section

```toml
[bucket]
naming_rules = "relaxed"
```

| Option | Default | Description |
|--------|---------|-------------|
| `naming_rules` | `relaxed` | `strict` for S3 DNS-compatible names, `relaxed` for more permissive |

## Logging Section

```toml
[logging]
level = "info"
format = "pretty"
```

| Option | Default | Description |
|--------|---------|-------------|
| `level` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `format` | `pretty` | Output format: `json` or `pretty` |

## API Section

```toml
[api]
compatibility_mode = "minio"
```

| Option | Default | Description |
|--------|---------|-------------|
| `compatibility_mode` | `minio` | API compatibility mode: `s3-strict`, `minio`, or `ceph` |

### Compatibility Modes

- **`s3-strict`**: Standard S3 API only. Maximum compatibility with AWS S3 clients.
- **`minio`**: S3 API plus MinIO extensions (health endpoints at `/minio/health/*`).
- **`ceph`**: S3 API with Ceph RGW compatibility. Enables full versioning support including `list_object_versions` and proper delete marker handling for compatibility with ceph/s3-tests.

## Environment Variables

The `RUST_LOG` environment variable overrides the configured log level:

```bash
RUST_LOG=debug rucket serve
```
