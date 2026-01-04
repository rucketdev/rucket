# Troubleshooting

Common issues and solutions.

## Connection Issues

### Connection Refused

```
error: Connection refused (os error 111)
```

**Causes:**
- Service not running
- Wrong port or address
- Firewall blocking

**Solutions:**
```bash
# Check if service is running
systemctl status rucket

# Verify listening port
ss -tlnp | grep rucket

# Test local connectivity
curl http://localhost:9000/minio/health/live

# Check firewall
iptables -L -n | grep 9000
```

### Connection Timeout

**Causes:**
- Network routing issue
- Firewall rules
- Service overloaded

**Solutions:**
```bash
# Test network path
traceroute <rucket-host>

# Check service health
curl -m 5 http://<rucket-host>:9000/minio/health/ready

# Check system resources
top -p $(pgrep rucket)
```

## Authentication Errors

### 403 Forbidden / InvalidAccessKeyId

```xml
<Error>
  <Code>InvalidAccessKeyId</Code>
</Error>
```

**Causes:**
- Wrong access key
- Credential mismatch between client and server

**Solutions:**
```bash
# Verify server credentials
grep access_key /etc/rucket/rucket.toml

# Check environment override
echo $RUCKET__AUTH__ACCESS_KEY

# Verify client configuration
aws configure list
```

### SignatureDoesNotMatch

```xml
<Error>
  <Code>SignatureDoesNotMatch</Code>
</Error>
```

**Causes:**
- Wrong secret key
- Clock skew between client and server
- Request body modified in transit

**Solutions:**
```bash
# Check time sync
timedatectl status
chronyc tracking

# Verify secret key matches
# Client:
aws configure get aws_secret_access_key

# Server:
grep secret_key /etc/rucket/rucket.toml
```

## Storage Errors

### NoSuchBucket

```xml
<Error>
  <Code>NoSuchBucket</Code>
</Error>
```

**Causes:**
- Bucket doesn't exist
- Case sensitivity mismatch

**Solutions:**
```bash
# List existing buckets
aws s3 ls --endpoint-url http://localhost:9000

# Check exact bucket name (case sensitive)
ls /var/lib/rucket/data/
```

### Disk Full

```
Error: No space left on device
```

**Solutions:**
```bash
# Check disk usage
df -h /var/lib/rucket

# Find large files
du -sh /var/lib/rucket/data/* | sort -h

# Clean incomplete multipart uploads
aws s3api list-multipart-uploads --bucket <bucket> \
  --endpoint-url http://localhost:9000

# Abort stale uploads
aws s3api abort-multipart-upload --bucket <bucket> \
  --key <key> --upload-id <id> --endpoint-url http://localhost:9000
```

## Performance Issues

### Slow Requests

**Diagnosis:**
```bash
# Check request latency metrics
curl -s http://localhost:9001/metrics | grep request_duration

# Check system I/O
iostat -x 1

# Check for disk contention
iotop
```

**Solutions:**
- Increase redb cache: `RUCKET__STORAGE__REDB__CACHE_SIZE_BYTES=134217728`
- Switch to periodic sync: `RUCKET__STORAGE__SYNC__DATA=periodic`
- Use faster storage (SSD/NVMe)

### High Memory Usage

**Diagnosis:**
```bash
# Check process memory
ps aux | grep rucket
cat /proc/$(pgrep rucket)/status | grep -i mem
```

**Solutions:**
- Reduce redb cache size
- Check for memory leaks in logs
- Limit concurrent connections at reverse proxy

## WAL Issues

### WAL Corruption Detected

```
ERROR WAL entry CRC32 mismatch - corruption detected
```

**Causes:**
- Disk corruption
- Unclean shutdown during write
- Hardware failure

**Solutions:**
```bash
# Check disk health
smartctl -a /dev/sda

# Recovery will use entries before corruption
# Review recovery stats in logs
journalctl -u rucket | grep -i recovery

# If data is critical, restore from backup
```

### WAL Growing Large

**Causes:**
- Checkpoint not running
- High write volume

**Solutions:**
```bash
# Check checkpoint configuration
grep checkpoint /etc/rucket/rucket.toml

# Force rotation (via server restart)
systemctl restart rucket

# Check WAL size
ls -lh /var/lib/rucket/data/wal/
```

## TLS Issues

### Certificate Errors

```
error: TLS handshake failed
```

**Solutions:**
```bash
# Verify certificate
openssl x509 -in /etc/rucket/certs/server.crt -text -noout

# Check expiration
openssl x509 -in /etc/rucket/certs/server.crt -noout -dates

# Test TLS connection
openssl s_client -connect localhost:9000

# Verify key matches certificate
openssl x509 -noout -modulus -in server.crt | md5sum
openssl rsa -noout -modulus -in server.key | md5sum
```

## Service Won't Start

### Check Logs First

```bash
journalctl -u rucket -n 50 --no-pager
```

### Common Causes

**Port already in use:**
```bash
ss -tlnp | grep 9000
# Kill conflicting process or change port
```

**Permission denied:**
```bash
# Check data directory ownership
ls -la /var/lib/rucket
chown -R rucket:rucket /var/lib/rucket
```

**Invalid configuration:**
```bash
# Validate config syntax
rucket serve --config /etc/rucket/rucket.toml --dry-run
```

**Missing dependencies:**
```bash
# Check binary dependencies
ldd /usr/local/bin/rucket
```

## Debug Mode

Enable verbose logging for troubleshooting:

```bash
# Environment variable
RUST_LOG=debug rucket serve

# Or in config
[logging]
level = "debug"
```

For specific components:
```bash
RUST_LOG=rucket_storage=trace,rucket_api=debug rucket serve
```
