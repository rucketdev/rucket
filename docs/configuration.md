# Configuration

Rucket is configured via TOML file or environment variables.

## Quick Start

```bash
# No config needed - sensible defaults
rucket serve

# With config file
rucket serve --config rucket.toml
```

Default config locations: `./rucket.toml`, `/etc/rucket/rucket.toml`

## Environment Variables

All options can be set via environment variables with `RUCKET__` prefix:

```bash
RUCKET__SERVER__BIND=0.0.0.0:9000
RUCKET__AUTH__ACCESS_KEY=mykey
RUCKET__AUTH__SECRET_KEY=mysecret
RUCKET__STORAGE__DATA_DIR=/var/lib/rucket
RUCKET__LOGGING__LEVEL=debug
RUCKET__METRICS__ENABLED=false
```

Use double underscore (`__`) for nested options:
```bash
RUCKET__STORAGE__WAL__ENABLED=false
RUCKET__STORAGE__SYNC__DATA=always
```

---

## Full Reference

### [server]

```toml
[server]
bind = "127.0.0.1:9000"
tls_cert = "/path/to/cert.pem"  # optional
tls_key = "/path/to/key.pem"    # optional
max_body_size = 5368709120       # 5 GiB, 0 = unlimited
```

| Option | Default | Description |
|--------|---------|-------------|
| `bind` | `127.0.0.1:9000` | Address and port |
| `tls_cert` | none | Path to TLS certificate |
| `tls_key` | none | Path to TLS private key |
| `max_body_size` | `5368709120` | Max request body (bytes) |

### [storage]

```toml
[storage]
data_dir = "./data"
temp_dir = "./data/.tmp"  # optional, defaults to data_dir/.tmp
```

| Option | Default | Description |
|--------|---------|-------------|
| `data_dir` | `./data` | Object data and metadata directory |
| `temp_dir` | `{data_dir}/.tmp` | Temporary files directory |

See [DURABILITY.md](DURABILITY.md) for detailed durability configuration.

#### [storage.redb]

```toml
[storage.redb]
cache_size_bytes = 67108864  # 64 MiB
```

#### [storage.wal]

```toml
[storage.wal]
enabled = true
sync_mode = "fdatasync"    # none | fdatasync | fsync
recovery_mode = "light"    # light | full

[storage.wal.checkpoint]
entries_threshold = 10000
bytes_threshold = 67108864  # 64 MB
interval_ms = 60000         # 1 minute
```

#### [storage.sync]

```toml
[storage.sync]
data = "periodic"           # none | periodic | threshold | always
metadata = "always"
interval_ms = 1000
bytes_threshold = 10485760  # 10 MB
ops_threshold = 100
verify_checksums_on_read = false
```

### [auth]

```toml
[auth]
access_key = "rucket"
secret_key = "rucket123"
```

| Option | Default | Description |
|--------|---------|-------------|
| `access_key` | `rucket` | AWS-compatible access key ID |
| `secret_key` | `rucket123` | AWS-compatible secret key |

### [bucket]

```toml
[bucket]
naming_rules = "relaxed"  # strict | relaxed
```

| Option | Default | Description |
|--------|---------|-------------|
| `naming_rules` | `relaxed` | `strict` = S3 DNS-compatible only |

### [logging]

```toml
[logging]
level = "info"
format = "pretty"      # pretty | json
log_requests = true
```

| Option | Default | Description |
|--------|---------|-------------|
| `level` | `info` | trace, debug, info, warn, error |
| `format` | `pretty` | Output format |
| `log_requests` | `true` | Log HTTP requests |

Override with `RUST_LOG` environment variable.

### [metrics]

```toml
[metrics]
enabled = true
port = 9001
bind = "0.0.0.0"
include_storage_metrics = true
storage_metrics_interval_secs = 60
```

| Option | Default | Description |
|--------|---------|-------------|
| `enabled` | `true` | Enable Prometheus metrics |
| `port` | `9001` | Metrics endpoint port |
| `bind` | `0.0.0.0` | Metrics bind address |
| `include_storage_metrics` | `true` | Include bucket/object counts |
| `storage_metrics_interval_secs` | `60` | Storage metrics refresh interval |

Access at `http://{bind}:{port}/metrics`

### [api]

```toml
[api]
compatibility_mode = "minio"  # s3-strict | minio | ceph
```

| Mode | Description |
|------|-------------|
| `s3-strict` | Standard S3 API only |
| `minio` | S3 + MinIO health endpoints (`/minio/health/*`) |
| `ceph` | S3 + Ceph RGW compatibility (versioning, delete markers) |

### [cluster]

Enable distributed mode with Raft consensus for metadata replication.

```toml
[cluster]
enabled = true
node_id = 1
bind_cluster = "0.0.0.0:9001"
peers = ["node2:9001", "node3:9001"]
raft_log_dir = "./data/raft"

[cluster.raft]
heartbeat_interval_ms = 150
election_timeout_min_ms = 300
election_timeout_max_ms = 600

[cluster.bootstrap]
enabled = true        # Only on first node!
expect_nodes = 3
timeout_secs = 60
```

| Option | Default | Description |
|--------|---------|-------------|
| `enabled` | `false` | Enable distributed cluster mode |
| `node_id` | `1` | Unique node ID (must be stable across restarts) |
| `bind_cluster` | `127.0.0.1:9001` | Address for Raft RPC communication |
| `peers` | `[]` | Static list of peer addresses |
| `raft_log_dir` | `./data/raft` | Path for Raft log storage |

#### [cluster.raft]

```toml
[cluster.raft]
heartbeat_interval_ms = 150
election_timeout_min_ms = 300
election_timeout_max_ms = 600
snapshot_threshold = 10000
max_in_snapshot_log_to_keep = 1000
```

| Option | Default | Description |
|--------|---------|-------------|
| `heartbeat_interval_ms` | `150` | Leader heartbeat interval |
| `election_timeout_min_ms` | `300` | Minimum election timeout |
| `election_timeout_max_ms` | `600` | Maximum election timeout |
| `snapshot_threshold` | `10000` | Log entries before snapshot |

#### [cluster.bootstrap]

Bootstrap configuration for initial cluster formation.

| Option | Default | Description |
|--------|---------|-------------|
| `enabled` | `false` | Bootstrap a new cluster (only ONE node!) |
| `expect_nodes` | `1` | Wait for this many nodes before forming cluster |
| `timeout_secs` | `60` | Timeout waiting for expected nodes |

#### [cluster.discover]

Alternative to static `peers` list. Supports multiple discovery methods:

**DNS Discovery:**
```toml
[cluster.discover]
type = "dns"
hostname = "rucket.local"
port = 9001
use_srv = false
```

**Kubernetes Discovery:**
```toml
[cluster.discover]
type = "kubernetes"
service = "rucket-headless"
namespace = "default"
port = "raft"
```

**Cloud Auto-Discovery:**
```toml
[cluster.discover]
type = "cloud"
cluster_tag = "rucket:cluster"
cluster_value = "production"
raft_port = 9001
# AWS-specific
aws_use_imdsv2 = true
aws_region = "us-west-2"
```

**Gossip Discovery (SPOF-free):**
```toml
[cluster.discover]
type = "gossip"
bind = "0.0.0.0:9002"
bootstrap_peers = ["seed1:9002", "seed2:9002"]
encrypt = false
```

---

## Example Configurations

### Development

```toml
[server]
bind = "127.0.0.1:9000"

[storage]
data_dir = "./data"

[logging]
level = "debug"
```

### Production

```toml
[server]
bind = "0.0.0.0:9000"
tls_cert = "/etc/rucket/cert.pem"
tls_key = "/etc/rucket/key.pem"

[storage]
data_dir = "/var/lib/rucket/data"

[storage.wal]
enabled = true
sync_mode = "fsync"
recovery_mode = "full"

[auth]
access_key = "${RUCKET_ACCESS_KEY}"
secret_key = "${RUCKET_SECRET_KEY}"

[logging]
level = "info"
format = "json"

[metrics]
enabled = true
port = 9001
```

### Docker

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e RUCKET__SERVER__BIND=0.0.0.0:9000 \
  -e RUCKET__AUTH__ACCESS_KEY=mykey \
  -e RUCKET__AUTH__SECRET_KEY=mysecret \
  -v rucket-data:/data \
  ghcr.io/rucketdev/rucket:latest
```
