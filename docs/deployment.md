# Deployment Guide

This guide covers common deployment patterns for Rucket in production environments.

## Systemd Service

Create `/etc/systemd/system/rucket.service`:

```ini
[Unit]
Description=Rucket S3-compatible Object Storage
After=network.target
Documentation=https://github.com/rucketdev/rucket

[Service]
Type=simple
User=rucket
Group=rucket
ExecStart=/usr/local/bin/rucket serve --config /etc/rucket/rucket.toml
Restart=on-failure
RestartSec=5

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/rucket
PrivateTmp=true

# Resource limits (adjust as needed)
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Setup:

```bash
# Create user and directories
sudo useradd -r -s /bin/false rucket
sudo mkdir -p /var/lib/rucket /etc/rucket
sudo chown rucket:rucket /var/lib/rucket

# Copy configuration
sudo cp rucket.sample.toml /etc/rucket/rucket.toml
sudo chmod 600 /etc/rucket/rucket.toml

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable rucket
sudo systemctl start rucket
```

## Docker Compose

Create `docker-compose.yml`:

```yaml
services:
  rucket:
    image: ghcr.io/rucketdev/rucket:latest
    ports:
      - "9000:9000"
      - "9001:9001"  # Metrics
    volumes:
      - rucket-data:/data
      - ./rucket.toml:/etc/rucket/rucket.toml:ro
    environment:
      - RUST_LOG=info
    restart: unless-stopped
    # Optional: resource limits
    deploy:
      resources:
        limits:
          memory: 2G

volumes:
  rucket-data:
```

Start with:

```bash
docker compose up -d
docker compose logs -f rucket
```

## Reverse Proxy (nginx)

For TLS termination and load balancing:

```nginx
upstream rucket {
    server 127.0.0.1:9000;
    keepalive 32;
}

server {
    listen 443 ssl http2;
    server_name s3.example.com;

    ssl_certificate /etc/letsencrypt/live/s3.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/s3.example.com/privkey.pem;

    # S3 requires large uploads
    client_max_body_size 5G;

    location / {
        proxy_pass http://rucket;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Connection "";

        # For streaming uploads
        proxy_request_buffering off;
        proxy_buffering off;
    }
}
```

## Production Configuration Tips

### Durability

For production data, use the `durable` preset:

```toml
[storage]
durability = "durable"
```

This enables:
- Synchronous writes
- WAL with fsync
- Full checksum verification

See [DURABILITY.md](DURABILITY.md) for details.

### Monitoring

Enable Prometheus metrics:

```toml
[metrics]
enabled = true
port = 9001
```

Key metrics to monitor:
- `rucket_requests_total` - Request count by operation
- `rucket_request_duration_seconds` - Latency histogram
- `rucket_storage_bytes` - Storage usage
- `rucket_objects_total` - Object count

### Logging

Configure structured logging:

```toml
[logging]
level = "info"
format = "json"
```

### Security Checklist

- [ ] Use strong credentials (not defaults)
- [ ] Enable TLS or use reverse proxy with TLS
- [ ] Restrict network access with firewall
- [ ] Run as non-root user
- [ ] Set appropriate file permissions on data directory
- [ ] Regular backups of `/var/lib/rucket`

## Health Checks

Rucket responds to GET requests on any bucket path. For container orchestration:

```bash
# Simple health check
curl -f http://localhost:9000/ || exit 1
```

For Kubernetes liveness probe:

```yaml
livenessProbe:
  httpGet:
    path: /
    port: 9000
  initialDelaySeconds: 5
  periodSeconds: 10
```
