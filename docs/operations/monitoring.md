# Monitoring

Rucket exposes Prometheus metrics on a dedicated port (default: 9001).

## Enabling Metrics

```toml
[metrics]
enabled = true
port = 9001
bind = "0.0.0.0"
include_storage_metrics = true
storage_metrics_interval_secs = 60
```

Or via environment variables:
```bash
RUCKET__METRICS__ENABLED=true
RUCKET__METRICS__PORT=9001
```

## Available Metrics

### Request Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `rucket_requests_total` | Counter | `operation`, `status`, `method` | Total S3 API requests |
| `rucket_request_duration_seconds` | Histogram | `operation` | Request latency |
| `rucket_request_bytes_total` | Counter | `operation` | Bytes received |
| `rucket_response_bytes_total` | Counter | `operation` | Bytes sent |
| `rucket_errors_total` | Counter | `operation`, `error_code` | Errors by type |

### Storage Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `rucket_buckets_total` | Gauge | Number of buckets |
| `rucket_objects_total` | Gauge | Number of objects |
| `rucket_storage_bytes_total` | Gauge | Total storage used (bytes) |
| `rucket_multipart_uploads_active` | Gauge | Active multipart uploads |

Storage metrics are refreshed based on `storage_metrics_interval_secs`.

## Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'rucket'
    static_configs:
      - targets: ['rucket:9001']
    scrape_interval: 15s
```

## Grafana Dashboard

Sample dashboard JSON for import:

```json
{
  "title": "Rucket",
  "panels": [
    {
      "title": "Requests/sec",
      "targets": [{
        "expr": "rate(rucket_requests_total[5m])"
      }]
    },
    {
      "title": "Request Latency (p99)",
      "targets": [{
        "expr": "histogram_quantile(0.99, rate(rucket_request_duration_seconds_bucket[5m]))"
      }]
    },
    {
      "title": "Error Rate",
      "targets": [{
        "expr": "rate(rucket_errors_total[5m])"
      }]
    },
    {
      "title": "Storage Used",
      "targets": [{
        "expr": "rucket_storage_bytes_total"
      }]
    },
    {
      "title": "Objects Count",
      "targets": [{
        "expr": "rucket_objects_total"
      }]
    }
  ]
}
```

## Key Queries

### Request Rate by Operation
```promql
sum(rate(rucket_requests_total[5m])) by (operation)
```

### Error Rate
```promql
sum(rate(rucket_errors_total[5m])) / sum(rate(rucket_requests_total[5m]))
```

### P99 Latency by Operation
```promql
histogram_quantile(0.99, sum(rate(rucket_request_duration_seconds_bucket[5m])) by (le, operation))
```

### Throughput
```promql
sum(rate(rucket_response_bytes_total[5m]))
```

## Alerting Rules

```yaml
groups:
  - name: rucket
    rules:
      - alert: RucketHighErrorRate
        expr: sum(rate(rucket_errors_total[5m])) / sum(rate(rucket_requests_total[5m])) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate ({{ $value | humanizePercentage }})"

      - alert: RucketHighLatency
        expr: histogram_quantile(0.99, sum(rate(rucket_request_duration_seconds_bucket[5m])) by (le)) > 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "P99 latency above 1s"

      - alert: RucketDown
        expr: up{job="rucket"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Rucket instance down"
```

## Health Endpoints

```bash
# Liveness (is the process running)
curl http://localhost:9000/minio/health/live

# Readiness (is it accepting requests)
curl http://localhost:9000/minio/health/ready
```

## Logging

Configure JSON logging for log aggregation:

```toml
[logging]
level = "info"
format = "json"
log_requests = true
```

Or:
```bash
RUCKET__LOGGING__FORMAT=json
RUCKET__LOGGING__LEVEL=info
```

Use `RUST_LOG` for fine-grained control:
```bash
RUST_LOG=rucket=debug,rucket_storage=trace
```
