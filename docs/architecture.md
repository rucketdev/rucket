# Architecture Overview

Rucket is designed as a modular, single-node S3-compatible object storage server.

## Crate Structure

```
rucket/
├── rucket          # Main binary, CLI, server startup
├── rucket-api      # HTTP handlers, S3 API implementation
├── rucket-storage  # Storage backend, metadata management
└── rucket-core     # Shared types, error handling, configuration
```

## Data Flow

```
Client Request
      │
      ▼
┌─────────────┐
│  axum HTTP  │  (rucket-api)
│   Router    │
└─────────────┘
      │
      ▼
┌─────────────┐
│  Handlers   │  Bucket, Object, Multipart
└─────────────┘
      │
      ▼
┌─────────────┐
│  Storage    │  (rucket-storage)
│  Backend    │
└─────────────┘
      │
      ├──────────────┐
      ▼              ▼
┌──────────┐   ┌──────────┐
│  SQLite  │   │   File   │
│ Metadata │   │  System  │
└──────────┘   └──────────┘
```

## Storage Design

### Object Storage

Objects are stored as flat files with UUID-based names:

```
data/
├── my-bucket/
│   ├── 550e8400-e29b-41d4-a716-446655440000.dat
│   ├── 6ba7b810-9dad-11d1-80b4-00c04fd430c8.dat
│   └── ...
└── metadata.db
```

This design avoids issues with special characters in object keys (slashes, unicode, etc.).

### Metadata Database

SQLite stores all metadata:

- **buckets**: Bucket name, creation time
- **objects**: Key-to-UUID mapping, size, ETag, content type
- **multipart_uploads**: In-progress uploads and their parts

### Concurrency

Write operations use atomic patterns:

1. Write data to temporary file
2. Compute ETag (MD5 hash)
3. Begin SQLite transaction
4. Update metadata
5. Rename temp file to final location
6. Commit transaction

## Authentication

AWS Signature V4 authentication with static credentials from configuration.

## Error Handling

All S3 errors follow the standard XML error response format with appropriate HTTP status codes.
