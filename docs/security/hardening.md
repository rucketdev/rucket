# Security Hardening

Checklist for securing Rucket in production environments.

## Authentication

### Strong Credentials

Generate secure credentials:
```bash
# Generate access key (20 chars)
ACCESS_KEY=$(openssl rand -base64 15 | tr -dc 'A-Za-z0-9' | head -c 20)

# Generate secret key (40 chars)
SECRET_KEY=$(openssl rand -base64 30 | tr -dc 'A-Za-z0-9' | head -c 40)
```

Never use default credentials in production:
```toml
[auth]
access_key = "${RUCKET_ACCESS_KEY}"  # From environment
secret_key = "${RUCKET_SECRET_KEY}"
```

### Credential Rotation

1. Generate new credentials
2. Update client applications
3. Update Rucket configuration
4. Restart Rucket
5. Verify access with new credentials

## TLS Configuration

### Certificate Setup

```toml
[server]
bind = "0.0.0.0:9000"
tls_cert = "/etc/rucket/certs/server.crt"
tls_key = "/etc/rucket/certs/server.key"
```

Generate self-signed certificate (testing only):
```bash
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt \
  -days 365 -nodes -subj "/CN=rucket.local"
```

For production, use certificates from a trusted CA or Let's Encrypt.

### File Permissions

```bash
chmod 600 /etc/rucket/certs/server.key
chmod 644 /etc/rucket/certs/server.crt
chown rucket:rucket /etc/rucket/certs/*
```

## Server-Side Encryption (SSE-S3)

Enable encryption at rest:

```bash
# Generate 256-bit master key
MASTER_KEY=$(openssl rand -hex 32)
```

```toml
[storage.encryption]
enabled = true
master_key = "${RUCKET_MASTER_KEY}"
```

Store the master key securely (e.g., HashiCorp Vault, AWS Secrets Manager). Loss of the master key means loss of all encrypted data.

## Network Security

### Bind to Specific Interface

Don't bind to all interfaces unless required:
```toml
[server]
bind = "10.0.0.5:9000"  # Internal network only
```

### Firewall Rules

```bash
# Allow S3 traffic from trusted networks only
iptables -A INPUT -p tcp --dport 9000 -s 10.0.0.0/8 -j ACCEPT
iptables -A INPUT -p tcp --dport 9000 -j DROP

# Restrict metrics to internal monitoring
iptables -A INPUT -p tcp --dport 9001 -s 10.0.0.0/8 -j ACCEPT
iptables -A INPUT -p tcp --dport 9001 -j DROP
```

### Reverse Proxy

Place Rucket behind a reverse proxy for:
- TLS termination
- Rate limiting
- Request filtering
- Access logging

Nginx example:
```nginx
server {
    listen 443 ssl;
    server_name s3.example.com;

    ssl_certificate /etc/nginx/certs/server.crt;
    ssl_certificate_key /etc/nginx/certs/server.key;
    ssl_protocols TLSv1.2 TLSv1.3;

    client_max_body_size 5g;

    location / {
        proxy_pass http://127.0.0.1:9000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

## File System Security

### Data Directory

```bash
# Create dedicated user
useradd -r -s /sbin/nologin rucket

# Set ownership
chown -R rucket:rucket /var/lib/rucket

# Restrict permissions
chmod 700 /var/lib/rucket
```

### Run as Non-Root

systemd service:
```ini
[Service]
User=rucket
Group=rucket
```

Docker:
```bash
docker run --user 65534:65534 ...  # nonroot in distroless
```

## Request Limits

### Body Size Limit

```toml
[server]
max_body_size = 5368709120  # 5 GiB (S3 max for single PUT)
```

For stricter limits:
```toml
max_body_size = 104857600  # 100 MB
```

## Bucket Policies

Use bucket policies to restrict access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyPublicRead",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::sensitive-bucket/*",
      "Condition": {
        "Bool": {"aws:SecureTransport": "false"}
      }
    }
  ]
}
```

## Monitoring and Auditing

### Enable Request Logging

```toml
[logging]
level = "info"
format = "json"
log_requests = true
```

### Monitor for Anomalies

- Unusual request patterns
- Failed authentication attempts
- Large data transfers
- Access from unexpected IPs

## Security Checklist

- [ ] Strong, unique credentials configured
- [ ] TLS enabled with valid certificate
- [ ] Server-side encryption enabled
- [ ] Data directory permissions restricted (700)
- [ ] Running as non-root user
- [ ] Firewall rules configured
- [ ] Metrics endpoint not publicly accessible
- [ ] Request logging enabled
- [ ] Backup encryption key stored securely
- [ ] Regular credential rotation scheduled
