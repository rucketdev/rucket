# Docker Deployment

Deploy Rucket using Docker for simple containerized object storage.

## Quick Start

```bash
# Run with default settings
docker run -d \
  --name rucket \
  -p 9000:9000 \
  -p 9001:9001 \
  -v rucket-data:/data \
  ghcr.io/rucketdev/rucket:latest
```

Access at `http://localhost:9000` with default credentials (`rucket`/`rucket123`).

## Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  rucket:
    image: ghcr.io/rucketdev/rucket:latest
    container_name: rucket
    restart: unless-stopped
    ports:
      - "9000:9000"  # S3 API
      - "9001:9001"  # Metrics
    volumes:
      - rucket-data:/data
      - ./rucket.toml:/etc/rucket/rucket.toml:ro  # Optional config
    environment:
      - RUCKET__SERVER__BIND=0.0.0.0:9000
      - RUCKET__AUTH__ACCESS_KEY=${RUCKET_ACCESS_KEY:-rucket}
      - RUCKET__AUTH__SECRET_KEY=${RUCKET_SECRET_KEY:-rucket123}
      - RUCKET__METRICS__BIND=0.0.0.0
    healthcheck:
      test: ["CMD", "/rucket", "health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s

volumes:
  rucket-data:
```

Start with:
```bash
docker compose up -d
```

## Configuration via Environment Variables

All configuration options can be set via environment variables with the `RUCKET__` prefix:

```bash
docker run -d \
  --name rucket \
  -p 9000:9000 \
  -p 9001:9001 \
  -e RUCKET__SERVER__BIND=0.0.0.0:9000 \
  -e RUCKET__AUTH__ACCESS_KEY=myaccesskey \
  -e RUCKET__AUTH__SECRET_KEY=mysecretkey \
  -e RUCKET__LOGGING__LEVEL=info \
  -e RUCKET__LOGGING__FORMAT=json \
  -e RUCKET__METRICS__ENABLED=true \
  -v rucket-data:/data \
  ghcr.io/rucketdev/rucket:latest
```

See [Configuration Reference](../configuration.md) for all options.

## Production Configuration

### With TLS

```yaml
services:
  rucket:
    image: ghcr.io/rucketdev/rucket:latest
    ports:
      - "9000:9000"
    volumes:
      - rucket-data:/data
      - ./certs:/certs:ro
    environment:
      - RUCKET__SERVER__BIND=0.0.0.0:9000
      - RUCKET__SERVER__TLS_CERT=/certs/cert.pem
      - RUCKET__SERVER__TLS_KEY=/certs/key.pem
      - RUCKET__AUTH__ACCESS_KEY=${RUCKET_ACCESS_KEY}
      - RUCKET__AUTH__SECRET_KEY=${RUCKET_SECRET_KEY}
```

### With Server-Side Encryption

```bash
# Generate master key
MASTER_KEY=$(openssl rand -hex 32)

docker run -d \
  --name rucket \
  -p 9000:9000 \
  -e RUCKET__STORAGE__ENCRYPTION__ENABLED=true \
  -e RUCKET__STORAGE__ENCRYPTION__MASTER_KEY=${MASTER_KEY} \
  -e RUCKET__AUTH__ACCESS_KEY=mykey \
  -e RUCKET__AUTH__SECRET_KEY=mysecret \
  -v rucket-data:/data \
  ghcr.io/rucketdev/rucket:latest
```

### Maximum Durability

```yaml
services:
  rucket:
    image: ghcr.io/rucketdev/rucket:latest
    environment:
      - RUCKET__STORAGE__WAL__ENABLED=true
      - RUCKET__STORAGE__WAL__SYNC_MODE=fsync
      - RUCKET__STORAGE__WAL__RECOVERY_MODE=full
      - RUCKET__STORAGE__SYNC__DATA=always
      - RUCKET__STORAGE__SYNC__VERIFY_CHECKSUMS_ON_READ=true
    volumes:
      - rucket-data:/data
```

## Health Checks

The container exposes health endpoints:

```bash
# Liveness check (MinIO compatibility)
curl http://localhost:9000/minio/health/live

# Readiness check
curl http://localhost:9000/minio/health/ready
```

Docker health check is built into the image:
```bash
docker inspect --format='{{.State.Health.Status}}' rucket
```

## Metrics

Prometheus metrics are available on port 9001:

```bash
curl http://localhost:9001/metrics
```

To scrape from Prometheus:
```yaml
scrape_configs:
  - job_name: 'rucket'
    static_configs:
      - targets: ['rucket:9001']
```

## Volumes and Persistence

| Path | Description |
|------|-------------|
| `/data` | Object data, metadata, and WAL |
| `/etc/rucket/rucket.toml` | Configuration file (optional) |

### Backup Considerations

The `/data` volume contains:
- `buckets/` - Bucket metadata
- `objects/` - Object data files
- `wal/` - Write-ahead log
- `metadata.redb` - redb database

For consistent backups, stop the container or use filesystem snapshots.

## Resource Limits

```yaml
services:
  rucket:
    image: ghcr.io/rucketdev/rucket:latest
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '0.5'
          memory: 256M
```

## Networking

### Behind a Reverse Proxy

```yaml
services:
  rucket:
    image: ghcr.io/rucketdev/rucket:latest
    # Only expose to internal network
    expose:
      - "9000"
    networks:
      - internal

  nginx:
    image: nginx:alpine
    ports:
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
    networks:
      - internal
      - external

networks:
  internal:
  external:
```

### Multiple Instances

For horizontal scaling (each instance with separate data):

```yaml
services:
  rucket-1:
    image: ghcr.io/rucketdev/rucket:latest
    ports:
      - "9000:9000"
    volumes:
      - rucket-1-data:/data

  rucket-2:
    image: ghcr.io/rucketdev/rucket:latest
    ports:
      - "9002:9000"
    volumes:
      - rucket-2-data:/data
```

## Building from Source

```bash
# Clone repository
git clone https://github.com/rucketdev/rucket.git
cd rucket

# Build image
docker build -t rucket:local .

# Run local build
docker run -p 9000:9000 -v rucket-data:/data rucket:local
```

## Troubleshooting

### Container won't start

Check logs:
```bash
docker logs rucket
```

### Permission denied errors

The distroless image runs as non-root. Ensure the data volume is writable:
```bash
docker run -v rucket-data:/data --entrypoint="" rucket:local ls -la /data
```

### Connection refused

Verify the container is binding to `0.0.0.0`, not `127.0.0.1`:
```bash
docker exec rucket cat /proc/1/cmdline
# Should show: --bind 0.0.0.0:9000
```

### High memory usage

Adjust redb cache size:
```bash
-e RUCKET__STORAGE__REDB__CACHE_SIZE_BYTES=33554432  # 32MB
```
