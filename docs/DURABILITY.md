# Durability and Data Integrity

Rucket provides configurable durability guarantees through multiple layers of protection: file synchronization, metadata persistence, write-ahead logging (WAL), and checksum verification.

> **Important**: This document describes Rucket's durability model based on current filesystem and storage research. For maximum durability, use the `Durable` preset with enterprise-grade SSDs that have power loss protection (PLP).

## Durability Presets

Choose a preset based on your workload requirements:

| Preset | Use Case | Data Sync | Metadata Sync | WAL |
|--------|----------|-----------|---------------|-----|
| **Performance** | Development, caching | None | None | Disabled |
| **Balanced** | General workloads | Periodic (100 ops) | Periodic (10 ops) | Enabled |
| **Durable** | Critical data | Every write | Every write | Enabled + Fsync |

### Configuration Example

```toml
[storage]
durability_preset = "durable"  # "performance" | "balanced" | "durable"
```

## Sync Strategies

Control how aggressively data is flushed to disk:

| Strategy | Description | Trade-off |
|----------|-------------|-----------|
| **None** | No explicit sync | Maximum throughput, data may be lost on crash |
| **Periodic** | Sync every N operations | Balance between performance and durability |
| **Threshold** | Sync when pending bytes exceed limit | Good for variable workloads |
| **Always** | Sync every operation | Maximum durability, lower throughput |

## Write-Ahead Logging (WAL)

The WAL provides crash recovery by logging operations before execution:

1. **Intent logged** - Operation is recorded to WAL
2. **Data written** - File is written to disk
3. **Metadata updated** - Object metadata stored in redb
4. **Commit logged** - Success recorded to WAL

On crash, incomplete operations are rolled back:
- Incomplete PUT: orphaned file is deleted
- Incomplete DELETE: file is kept (delete is undone)

### WAL Sync Modes

| Mode | Description |
|------|-------------|
| **None** | No WAL sync (relies on OS) |
| **Fdatasync** | Sync data only (faster) |
| **Fsync** | Sync data + metadata (safest) |

## Recovery Modes

Configure startup recovery behavior:

### Light Recovery (Default)

Fast startup using WAL-only recovery:
- Replays WAL to find incomplete operations
- Rolls back incomplete PUTs/DELETEs
- Does not scan filesystem for orphans

### Full Recovery

Thorough verification for maximum integrity:
- All Light recovery steps, plus:
- Scans filesystem for orphaned files (data without metadata)
- Verifies CRC32C checksums of all objects
- Reports data corruption (does not auto-delete)

```toml
[storage.wal]
enabled = true
recovery_mode = "full"  # "light" | "full"
```

## Checksum Verification

Rucket computes CRC32C checksums during writes and can verify them on reads:

### On Write
- CRC32C computed as data streams in
- Stored in object metadata

### On Read (Optional)
- When enabled, recomputes CRC32C on read
- Compares against stored checksum
- Returns `ChecksumMismatch` error if corrupted

Enable for maximum data integrity (enabled by default in Durable preset):

```toml
[storage.sync]
verify_checksums_on_read = true
```

## Durability Guarantees by Mode

Understanding the data loss windows for each mode:

| Mode | Data Loss Window | Directory Entry Loss | Recommended Use |
|------|------------------|---------------------|-----------------|
| **Durable** | ~0 (fsync every write) | ~0 (directory fsync) | Production critical data |
| **Balanced** | 100 ops or 10MB | Up to 30s (no dir sync) | General production |
| **Performance** | Up to 30s (OS buffer) | Up to 30s+ | Development, caching |

### What These Windows Mean

- **Data Loss Window**: Time between when a write is acknowledged and when it survives a power failure
- **Directory Entry Loss**: A file's data may be on disk, but the filename may not be visible after crash
- In `Durable` mode with `Always` sync, both windows are effectively zero

## Recommendations

### Development / Testing
```toml
[storage]
durability_preset = "performance"
```

### Production - General
```toml
[storage]
durability_preset = "balanced"
```

### Production - Critical Data
```toml
[storage]
durability_preset = "durable"

[storage.wal]
recovery_mode = "full"
```

## Metadata Storage

Object metadata is stored in [redb](https://github.com/cberner/redb), an embedded ACID-compliant database:

- Automatic crash recovery on startup
- Configurable cache size for read performance
- Supports concurrent readers

## Architecture

```
Write Path:
  Client Request
       │
       ▼
  ┌─────────────┐
  │  WAL Intent │  ◄── Logged before any writes
  └─────────────┘
       │
       ▼
  ┌─────────────┐
  │  Write Data │  ◄── Temp file + atomic rename
  └─────────────┘
       │
       ▼
  ┌─────────────┐
  │  Sync Data  │  ◄── Based on sync strategy
  └─────────────┘
       │
       ▼
  ┌─────────────┐
  │  Metadata   │  ◄── redb transaction
  └─────────────┘
       │
       ▼
  ┌─────────────┐
  │ WAL Commit  │  ◄── Operation complete
  └─────────────┘
       │
       ▼
  Response to Client
```

## Hardware Considerations

### SSD Power Loss Protection

**Critical for production deployments**: Not all SSDs honor flush commands properly.

Consumer SSDs often use DRAM write caches without battery/capacitor backup. Research has shown that some consumer NVMe SSDs lose data even after reporting successful flush operations.

**Recommendation**: Use enterprise SSDs with Power Loss Protection (PLP) for production deployments. Enterprise SSDs include capacitors that allow flushing DRAM cache to NAND flash during power loss.

Signs your SSD has PLP:
- Marketed as "enterprise" or "datacenter"
- Specifications mention "power loss protection" or "PLP"
- Has capacitors visible on the PCB (for advanced users)

### Filesystem Recommendations

- **ext4**: Well-tested, use `data=ordered` mount option (default)
- **XFS**: Good performance, well-tested durability
- **Btrfs**: Use with caution; COW may affect performance
- **ZFS**: Excellent durability, ensure `sync=standard`

## Known Limitations

Current limitations that may be addressed in future versions:

### Directory Sync in Balanced Mode

In `Balanced` mode, directory entries are not explicitly synced after file creation. This means:
- File data is synced periodically (every 100 ops or 10MB)
- But the filename→inode mapping may still be in OS buffer cache
- On crash, file data exists but may not be visible

**Workaround**: Use `Durable` mode for critical data, which syncs directories.

### fsync Error Handling

Currently, Rucket logs warnings on fsync failures but continues operation. Research indicates that after an fsync failure, the state of buffered writes is undefined on some filesystems.

**Workaround**: Monitor logs for fsync warnings; consider them critical.

## Comparison with Other Systems

| Aspect | Rucket | PostgreSQL | SQLite | LevelDB |
|--------|--------|------------|--------|---------|
| WAL | ✅ Intent/Commit | ✅ WAL | ✅ WAL | ✅ WAL |
| Atomic writes | ✅ Rename | ✅ Rename | ✅ Rename | ✅ Rename |
| Directory fsync | ⚠️ Durable only | ✅ Always | ✅ Always | ⚠️ Optional |
| Checksums | ✅ CRC32C | ✅ CRC32C | ✅ CRC32C | ✅ CRC32C |

## References

- [Durability: Linux File APIs](https://www.evanjones.ca/durability-filesystem.html) - Comprehensive guide on fsync semantics
- [Can Applications Recover from fsync Failures?](https://dl.acm.org/doi/10.1145/3450338) - ACM research on error handling
- [redb Design Document](https://github.com/cberner/redb/blob/master/docs/design.md) - Metadata store durability model
