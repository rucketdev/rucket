# systemd Deployment

Deploy Rucket as a systemd service on Linux.

## Installation

### From Binary

```bash
# Download latest release
curl -L https://github.com/rucketdev/rucket/releases/latest/download/rucket-linux-amd64 \
  -o /usr/local/bin/rucket
chmod +x /usr/local/bin/rucket

# Verify installation
rucket --version
```

### From Source

```bash
cargo install --path crates/rucket
# or
cargo build --release && cp target/release/rucket /usr/local/bin/
```

## System Setup

### Create User and Directories

```bash
# Create service user
useradd -r -s /sbin/nologin -d /var/lib/rucket rucket

# Create directories
mkdir -p /var/lib/rucket/data
mkdir -p /etc/rucket

# Set ownership
chown -R rucket:rucket /var/lib/rucket
chown -R rucket:rucket /etc/rucket

# Set permissions
chmod 700 /var/lib/rucket
chmod 755 /etc/rucket
```

## Configuration

Create `/etc/rucket/rucket.toml`:

```toml
[server]
bind = "0.0.0.0:9000"

[storage]
data_dir = "/var/lib/rucket/data"

[storage.wal]
enabled = true
sync_mode = "fdatasync"

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

## Unit File

Create `/etc/systemd/system/rucket.service`:

```ini
[Unit]
Description=Rucket S3-compatible object storage
Documentation=https://github.com/rucketdev/rucket
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=rucket
Group=rucket

# Configuration
Environment=RUCKET_ACCESS_KEY=your-access-key
Environment=RUCKET_SECRET_KEY=your-secret-key
EnvironmentFile=-/etc/rucket/rucket.env

# Execution
ExecStart=/usr/local/bin/rucket serve --config /etc/rucket/rucket.toml
ExecReload=/bin/kill -HUP $MAINPID

# Security
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true
ReadWritePaths=/var/lib/rucket

# Resource limits
LimitNOFILE=65535
LimitNPROC=4096

# Restart policy
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Environment File (Optional)

Create `/etc/rucket/rucket.env` for secrets:

```bash
RUCKET_ACCESS_KEY=your-access-key
RUCKET_SECRET_KEY=your-secret-key
RUCKET_MASTER_KEY=your-encryption-key
```

Secure it:
```bash
chmod 600 /etc/rucket/rucket.env
chown rucket:rucket /etc/rucket/rucket.env
```

## Service Management

```bash
# Reload systemd
systemctl daemon-reload

# Enable on boot
systemctl enable rucket

# Start service
systemctl start rucket

# Check status
systemctl status rucket

# View logs
journalctl -u rucket -f
```

## Log Management

### journald Configuration

View logs:
```bash
# Follow logs
journalctl -u rucket -f

# Last 100 lines
journalctl -u rucket -n 100

# Since boot
journalctl -u rucket -b

# JSON format (if configured)
journalctl -u rucket -o json
```

### Log Rotation

journald handles rotation automatically. Configure in `/etc/systemd/journald.conf`:

```ini
[Journal]
MaxRetentionSec=7day
MaxFileSec=1day
SystemMaxUse=1G
```

### Forward to External System

For JSON logs with rsyslog:
```
if $programname == 'rucket' then /var/log/rucket.log
& stop
```

## TLS with systemd

```ini
[Service]
Environment=RUCKET__SERVER__TLS_CERT=/etc/rucket/certs/server.crt
Environment=RUCKET__SERVER__TLS_KEY=/etc/rucket/certs/server.key
ReadWritePaths=/var/lib/rucket
ReadOnlyPaths=/etc/rucket/certs
```

## Multiple Instances

Create per-instance unit files:

`/etc/systemd/system/rucket@.service`:
```ini
[Unit]
Description=Rucket instance %i
After=network-online.target

[Service]
Type=simple
User=rucket
Group=rucket
ExecStart=/usr/local/bin/rucket serve --config /etc/rucket/rucket-%i.toml
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Manage instances:
```bash
systemctl start rucket@primary
systemctl start rucket@secondary
```

## Health Check Integration

Create a health check timer for monitoring:

`/etc/systemd/system/rucket-health.service`:
```ini
[Unit]
Description=Rucket health check

[Service]
Type=oneshot
ExecStart=/usr/bin/curl -sf http://localhost:9000/minio/health/ready
```

`/etc/systemd/system/rucket-health.timer`:
```ini
[Unit]
Description=Run Rucket health check

[Timer]
OnCalendar=*:*:0/30
Persistent=true

[Install]
WantedBy=timers.target
```

## Troubleshooting

### Service won't start

```bash
# Check for configuration errors
rucket serve --config /etc/rucket/rucket.toml --dry-run

# Check permissions
ls -la /var/lib/rucket
ls -la /etc/rucket
```

### Permission denied

```bash
# Verify SELinux (if enabled)
ausearch -m avc -ts recent

# Fix SELinux context
restorecon -Rv /var/lib/rucket
```

### Port already in use

```bash
# Find process using port
ss -tlnp | grep 9000

# Kill conflicting process or change port
```
