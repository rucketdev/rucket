# Performance Tuning

Optimize Rucket for different workloads.

## Durability vs Performance Trade-offs

Rucket provides three presets for common scenarios:

| Preset | Sync Behavior | Use Case |
|--------|---------------|----------|
| Performance | No fsync | Caching, ephemeral data |
| Balanced | Periodic sync (1s/10MB) | General workloads |
| Durable | Sync every write | Critical data |

### Performance Mode

Maximum throughput, accepts data loss on crash:

```toml
[storage.sync]
data = "none"
metadata = "periodic"
interval_ms = 5000

[storage.sync.batch]
enabled = true
max_batch_size = 256
max_batch_bytes = 67108864  # 64 MB
max_batch_delay_ms = 50

[storage.wal]
enabled = false
```

### Balanced Mode (Default)

Good throughput with bounded data loss window:

```toml
[storage.sync]
data = "periodic"
metadata = "always"
interval_ms = 1000
bytes_threshold = 10485760  # 10 MB
ops_threshold = 100

[storage.wal]
enabled = true
sync_mode = "fdatasync"
```

### Durable Mode

Maximum durability, reduced throughput:

```toml
[storage.sync]
data = "always"
metadata = "always"
verify_checksums_on_read = true

[storage.sync.batch]
enabled = false

[storage.wal]
enabled = true
sync_mode = "fsync"
recovery_mode = "full"
```

## Memory Tuning

### redb Cache

Increase for large datasets or frequent random access:

```toml
[storage.redb]
cache_size_bytes = 268435456  # 256 MB
```

Rule of thumb: 1-5% of total dataset size, up to available RAM.

### System Memory

Ensure adequate system memory for:
- redb cache
- File system page cache
- Request buffers (especially for large objects)

Recommended: 2GB minimum, 4GB+ for production.

## Disk I/O

### Storage Type Performance

| Storage | Expected IOPS | Best For |
|---------|---------------|----------|
| HDD | 100-200 | Archive, cold data |
| SATA SSD | 10,000-50,000 | General workloads |
| NVMe SSD | 100,000-500,000 | High-performance |

### File System

ext4 or XFS recommended. For XFS:
```bash
mkfs.xfs -f /dev/sdb
mount -o noatime,nodiratime /dev/sdb /var/lib/rucket
```

Disable atime updates:
```
/dev/sdb /var/lib/rucket xfs noatime,nodiratime 0 2
```

### I/O Scheduler

For SSDs:
```bash
echo none > /sys/block/sda/queue/scheduler
```

For HDDs:
```bash
echo mq-deadline > /sys/block/sda/queue/scheduler
```

## Network Tuning

### TCP Settings

For high-throughput scenarios:

```bash
# Increase buffer sizes
sysctl -w net.core.rmem_max=16777216
sysctl -w net.core.wmem_max=16777216
sysctl -w net.ipv4.tcp_rmem="4096 87380 16777216"
sysctl -w net.ipv4.tcp_wmem="4096 87380 16777216"

# Connection handling
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_max_syn_backlog=65535
```

### File Descriptors

Increase limits for many concurrent connections:

```bash
# /etc/security/limits.conf
rucket soft nofile 65535
rucket hard nofile 65535
```

Or in systemd:
```ini
[Service]
LimitNOFILE=65535
```

## Request Size Optimization

### Large Objects

For workloads with large objects (>100MB):

```toml
[server]
max_body_size = 5368709120  # 5 GB

[storage.sync]
# Batch multiple small writes
batch.enabled = true
batch.max_batch_bytes = 67108864  # 64 MB
```

### Small Objects

For workloads with many small objects (<1KB):

```toml
[storage.redb]
cache_size_bytes = 536870912  # 512 MB (more metadata caching)

[storage.sync]
# More frequent sync due to higher IOPS
interval_ms = 500
ops_threshold = 500
```

## Benchmarking

### Using warp

```bash
# Install warp
go install github.com/minio/warp@latest

# Run mixed workload
warp mixed --host localhost:9000 \
  --access-key rucket --secret-key rucket123 \
  --duration 60s --concurrent 32
```

### Using s3bench

```bash
s3bench -endpoint http://localhost:9000 \
  -accessKey rucket -secretKey rucket123 \
  -bucket test-bucket \
  -objectSize 1048576 \  # 1 MB
  -numClients 32 \
  -numSamples 1000
```

### Interpreting Results

Key metrics to monitor:
- **Throughput (MB/s)**: Sustained data transfer rate
- **IOPS**: Operations per second (especially for small objects)
- **Latency (p99)**: 99th percentile response time
- **Error rate**: Should be 0% under normal load

## Monitoring Performance

### Key Metrics

```promql
# Request throughput
sum(rate(rucket_requests_total[5m]))

# P99 latency
histogram_quantile(0.99, sum(rate(rucket_request_duration_seconds_bucket[5m])) by (le))

# Data throughput
sum(rate(rucket_response_bytes_total[5m])) / 1024 / 1024  # MB/s
```

### System Metrics

```bash
# I/O wait
iostat -x 1

# CPU usage
mpstat 1

# Memory pressure
vmstat 1
```

## Common Bottlenecks

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| High CPU | Encryption, checksums | Faster CPU, reduce verify_checksums_on_read |
| High I/O wait | Disk bottleneck | Faster storage, increase batch size |
| High memory | Large cache, many connections | Reduce cache, add RAM |
| High latency | fsync overhead | Switch to periodic sync |
| Connection errors | File descriptor limit | Increase LimitNOFILE |
