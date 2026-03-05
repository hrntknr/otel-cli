---
name: otel-cli
description: Inspects OpenTelemetry traces, logs, and metrics from a local OTLP server. Use when the user needs to debug distributed traces, inspect application logs, check metrics, or troubleshoot observability issues during development.
allowed-tools: Bash(otel-cli:*)
---

# otel-cli

An in-memory OTLP server with SQL querying. Start a local collector, ingest traces/logs/metrics, and query with standard SQL.

## Quick Start

```bash
# 1. Start server (background)
otel-cli server --no-tui &

# 2. App exports to localhost:4317 (gRPC) or localhost:4318 (HTTP)

# 3. Query
otel-cli sql "SELECT span_name, duration_ns FROM traces WHERE service_name = 'myapp'"

# 4. Cleanup
otel-cli shutdown
```

## SQL Queries (Primary Interface)

Use `otel-cli sql` for all querying. Powered by DataFusion (standard SQL). Use `--format jsonl` for programmatic processing.

```bash
otel-cli sql "SELECT * FROM traces WHERE service_name = 'myapp'" --format jsonl
otel-cli sql "SELECT * FROM traces WHERE attributes['http.method'] = 'GET'"
otel-cli sql "SELECT service_name, COUNT(*), AVG(duration_ns) FROM traces GROUP BY service_name"
otel-cli sql "SELECT * FROM logs WHERE severity = 'ERROR'"
otel-cli sql "SELECT * FROM metrics WHERE metric_name = 'http.duration'"
otel-cli sql -f "SELECT * FROM traces"   # Follow mode (real-time)
```

## Convenience Subcommands

Shorthand wrappers around SQL. Use `otel-cli sql` for anything beyond simple filtering.

```bash
otel-cli traces --service myapp --limit 50
otel-cli traces -f --service myapp              # Follow
otel-cli logs --severity ERROR --since 5m
otel-cli metrics --name http_requests_total
```

## Server Management

```bash
otel-cli status                                  # Show counts
otel-cli clear --traces --logs --metrics         # Reset state
otel-cli shutdown                                # Stop server
```

## SQL Schema

### traces

| Column | Type |
|---|---|
| `trace_id`, `span_id`, `parent_span_id` | Utf8 |
| `span_name`, `service_name`, `status_message` | Utf8 |
| `kind`, `status_code` | Int32 |
| `start_time`, `end_time`, `duration_ns` | UInt64 |
| `attributes`, `resource` | Map<Utf8, Utf8> |

### logs

| Column | Type |
|---|---|
| `timestamp` | UInt64 |
| `severity` | Utf8 |
| `severity_number` | Int32 |
| `body`, `service_name`, `trace_id`, `span_id` | Utf8 |
| `attributes`, `resource` | Map<Utf8, Utf8> |

### metrics

| Column | Type |
|---|---|
| `timestamp` | UInt64 |
| `metric_name`, `metric_type`, `service_name` | Utf8 |
| `value`, `sum` | Float64 |
| `count` | UInt64 |
| `attributes`, `resource` | Map<Utf8, Utf8> |

## Agent Tips

- Always use `--no-tui` when starting the server
- Use `--format jsonl` for programmatic processing
- Use `otel-cli clear` between test runs to reset state
- Prefer `otel-cli sql` over convenience subcommands — it supports full SQL (aggregation, joins, subqueries)
- Access map columns with bracket syntax: `attributes['http.method']`, `resource['service.name']`
- Store capacity: `--max-traces` (default 1000), `--max-spans`, `--max-logs`, `--max-metrics`
