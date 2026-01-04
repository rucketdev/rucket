# Backup and Restore

Procedures for backing up and restoring Rucket data.

## Data Layout

Rucket stores all data in the configured `data_dir`:

```
/var/lib/rucket/data/
├── buckets/           # Bucket metadata (redb)
├── <bucket-name>/     # Object data files (UUID.dat)
├── wal/               # Write-ahead log
│   └── current.wal
├── metadata.redb      # Metadata database
└── .tmp/              # Temporary files
```

## Backup Strategies

### Cold Backup (Recommended for Consistency)

Stop Rucket, copy data, restart:

```bash
# Stop service
systemctl stop rucket

# Create backup
tar -czf /backup/rucket-$(date +%Y%m%d-%H%M%S).tar.gz /var/lib/rucket/data

# Restart service
systemctl start rucket
```

### Warm Backup (rsync)

Use rsync for incremental backups with brief pause:

```bash
#!/bin/bash
BACKUP_DIR=/backup/rucket
DATA_DIR=/var/lib/rucket/data

# Initial sync (can run while service is up)
rsync -av --delete $DATA_DIR/ $BACKUP_DIR/

# Brief pause for final consistency
systemctl stop rucket
rsync -av --delete $DATA_DIR/ $BACKUP_DIR/
systemctl start rucket
```

### Filesystem Snapshots

For ZFS, LVM, or cloud block storage:

```bash
# ZFS
zfs snapshot rpool/rucket@backup-$(date +%Y%m%d)

# LVM
lvcreate -L10G -s -n rucket-snap /dev/vg/rucket
```

Snapshots provide point-in-time consistency without stopping the service.

### Continuous Backup (rsync daemon)

For ongoing replication to a remote server:

```bash
#!/bin/bash
# Run every 15 minutes via cron
rsync -avz --delete \
  /var/lib/rucket/data/ \
  backup-server:/backup/rucket/
```

## Restore Procedures

### Full Restore

```bash
# Stop service
systemctl stop rucket

# Remove existing data
rm -rf /var/lib/rucket/data/*

# Restore from backup
tar -xzf /backup/rucket-20240101-120000.tar.gz -C /

# Fix ownership
chown -R rucket:rucket /var/lib/rucket/data

# Start service
systemctl start rucket
```

### WAL Recovery

If the service crashed, WAL recovery happens automatically on startup:

```bash
# Check recovery status in logs
journalctl -u rucket | grep -i recovery
```

For manual recovery verification:
```bash
rucket serve --config /etc/rucket/rucket.toml --recovery-mode full
```

### Point-in-Time Recovery

WAL enables recovery to the last checkpoint:

1. Restore the latest backup
2. Restore WAL files from the backup period
3. Start Rucket - it will replay WAL entries

## Backup Best Practices

### Schedule

| Backup Type | Frequency | Retention |
|-------------|-----------|-----------|
| Full backup | Weekly | 4 weeks |
| Incremental | Daily | 7 days |
| Snapshots | Hourly | 24 hours |

### Verification

Regularly test restores:

```bash
# Restore to test environment
tar -xzf /backup/rucket-latest.tar.gz -C /tmp/rucket-test

# Start with test config
rucket serve --data-dir /tmp/rucket-test/data --port 9999

# Verify data integrity
aws s3 ls --endpoint-url http://localhost:9999 s3://
```

### Offsite Storage

Copy backups to remote storage:

```bash
# To S3-compatible storage (using a different Rucket instance or AWS)
aws s3 cp /backup/rucket-latest.tar.gz s3://backups/rucket/ \
  --endpoint-url https://backup-storage.example.com

# To remote server
scp /backup/rucket-latest.tar.gz backup@remote:/backups/
```

## Encryption Key Backup

If using SSE-S3 encryption, the master key is critical:

```bash
# Back up the key separately from data
echo "$RUCKET_MASTER_KEY" | gpg --symmetric > /secure/rucket-master-key.gpg

# Store in multiple secure locations
# - Hardware security module (HSM)
# - Secret management system (Vault, AWS Secrets Manager)
# - Encrypted offline storage
```

Without the master key, encrypted data is unrecoverable.

## Disaster Recovery

### Recovery Time Objectives

| Scenario | RTO | Procedure |
|----------|-----|-----------|
| Service crash | Minutes | Automatic WAL recovery |
| Data corruption | Hours | Restore from backup + WAL replay |
| Full data loss | Hours | Restore from offsite backup |
| Encryption key loss | N/A | Data unrecoverable |

### Runbook

1. Assess the failure (service down vs data corruption)
2. Stop the service if running
3. Identify the most recent valid backup
4. Restore data to a clean directory
5. Verify data integrity
6. Start the service
7. Validate functionality with test requests
8. Update monitoring and alerting
