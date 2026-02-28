---
name: otel-cli
description: Inspects OpenTelemetry traces, logs, and metrics from a local OTLP server. Use when the user needs to debug distributed traces, inspect application logs, check metrics, or troubleshoot observability issues during development.
allowed-tools: Bash(otel-cli:*)
---

# otel-cli

An in-memory OpenTelemetry (OTLP) server with querying capabilities. Use this skill to start a local OTLP collector, query traces/logs/metrics, and debug observability issues.

## Prerequisites

The `otel-cli` binary must be available in PATH. If it is not installed, run:

```bash
curl -fsSL https://raw.githubusercontent.com/hrntknr/otel-cli/main/install.sh | sh
```

## Workflow

1. **Start the server** to begin collecting OTLP data
2. Configure the application under test to export to `localhost:4317` (gRPC) or `localhost:4318` (HTTP)
3. **Query** traces, logs, or metrics to inspect the collected data
4. Use **follow mode** (`-f`) for real-time streaming

## Commands

### Server

Start the OTLP collector server. Use `--no-tui` when running from an agent context.

```bash
# Start headless server (recommended for agent use)
otel-cli server --no-tui

# Custom ports
otel-cli server --no-tui --grpc-addr 0.0.0.0:4317 --http-addr 0.0.0.0:4318 --query-addr 0.0.0.0:4319

# Increase store capacity
otel-cli server --no-tui --max-items 5000
```

### View (Attach TUI to Running Server)

Connect to an already running server and display the same interactive TUI.

```bash
# Attach to default server (localhost:4319)
otel-cli view

# Attach to a remote server
otel-cli view --server http://remote-host:4319

# Customize local store capacity
otel-cli view --max-items 500
```

### Query Traces

Retrieve distributed traces from the server.

```bash
# List recent traces
otel-cli trace

# Filter by service name
otel-cli trace --service myapp

# Filter by trace ID
otel-cli trace --trace-id abc123def456

# Filter by attributes
otel-cli trace --attribute http.method=GET --attribute http.status_code=500

# JSONL output for programmatic processing
otel-cli trace --format jsonl

# Time range queries
otel-cli trace --since 5m
otel-cli trace --since 1h --until 30m

# Follow new traces in real-time
otel-cli trace -f --service myapp

# Follow with full trace groups (not just new spans)
otel-cli trace -f --full

# Limit results
otel-cli trace --limit 50
```

### Query Logs

Retrieve logs from the server.

```bash
# List recent logs
otel-cli log

# Filter by severity (shows this level and above)
otel-cli log --severity ERROR

# Filter by service and attributes
otel-cli log --service myapp --attribute environment=staging

# Follow logs in real-time
otel-cli log -f

# JSONL output
otel-cli log --format jsonl --since 10m
```

### Query Metrics

Retrieve metrics from the server.

```bash
# List recent metrics
otel-cli metrics

# Filter by metric name
otel-cli metrics --name http_requests_total

# Filter by service
otel-cli metrics --service myapp

# Follow metrics in real-time
otel-cli metrics -f --format jsonl
```

### SQL Queries

Run SQL queries against collected telemetry data. Supports `traces`, `logs`, and `metrics` tables.

```bash
# Query traces with SQL
otel-cli sql "SELECT * FROM traces WHERE service_name = 'myapp'"

# Query logs by severity
otel-cli sql "SELECT * FROM logs WHERE severity >= 'ERROR'"

# Select specific columns
otel-cli sql "SELECT span_name, duration_ns FROM traces LIMIT 10"

# Filter by attributes using bracket syntax
otel-cli sql "SELECT * FROM traces WHERE attributes['http.method'] = 'GET'"

# LIKE / regex matching
otel-cli sql "SELECT * FROM logs WHERE body LIKE '%timeout%'"

# Follow mode
otel-cli sql -f "SELECT * FROM logs"

# CSV output
otel-cli sql "SELECT * FROM metrics" --format csv
```

### Clear Data

Clear stored data from the server.

```bash
# Clear all data
otel-cli clear --traces --logs --metrics

# Clear only traces
otel-cli clear --traces

# Clear only logs
otel-cli clear --logs
```

## Common Options

| Option                    | Description                           | Default                 |
| ------------------------- | ------------------------------------- | ----------------------- |
| `--server <ADDR>`         | Query server address                  | `http://localhost:4319` |
| `--service <NAME>`        | Filter by service name                | —                       |
| `--attribute <KEY=VALUE>` | Filter by attribute (repeatable)      | —                       |
| `--limit <N>`             | Maximum results                       | `100`                   |
| `--format <FORMAT>`       | Output format: `text`, `jsonl`, `csv` | `text`                  |
| `-f, --follow`            | Follow new data in real-time          | —                       |
| `--since <SPEC>`          | Start of time range                   | —                       |
| `--until <SPEC>`          | End of time range                     | —                       |

## Time Specifications

- **Relative:** `30s`, `5m`, `1h`, `2d` (interpreted as "now minus duration")
- **Absolute:** RFC3339 format, e.g. `2024-01-01T00:00:00Z`

## SQL Tables and Columns

### traces

| Column           | Description                |
| ---------------- | -------------------------- |
| `trace_id`       | Hex-encoded trace ID       |
| `span_id`        | Hex-encoded span ID        |
| `parent_span_id` | Hex-encoded parent span ID |
| `service_name`   | Service name from resource |
| `span_name`      | Span operation name        |
| `kind`           | Span kind                  |
| `status_code`    | Status code                |
| `start_time`     | Start timestamp (RFC3339)  |
| `end_time`       | End timestamp (RFC3339)    |
| `duration_ns`    | Duration in nanoseconds    |
| `resource`       | Resource attributes        |
| `attributes`     | Span attributes            |

### logs

| Column            | Description                      |
| ----------------- | -------------------------------- |
| `timestamp`       | Log timestamp (RFC3339)          |
| `severity`        | Severity text (e.g. INFO, ERROR) |
| `severity_number` | Numeric severity level           |
| `body`            | Log body                         |
| `service_name`    | Service name from resource       |
| `resource`        | Resource attributes              |
| `attributes`      | Log attributes                   |

### metrics

| Column         | Description                               |
| -------------- | ----------------------------------------- |
| `timestamp`    | Data point timestamp (RFC3339)            |
| `metric_name`  | Metric name                               |
| `type`         | Metric type (Gauge, Sum, Histogram, etc.) |
| `value`        | Data point value                          |
| `count`        | Histogram/summary count                   |
| `sum`          | Histogram/summary sum                     |
| `service_name` | Service name from resource                |
| `resource`     | Resource attributes                       |
| `attributes`   | Data point attributes                     |

### WHERE clause operators

- Comparison: `=`, `!=`, `<`, `>`, `<=`, `>=`
- Pattern matching: `LIKE`, `NOT LIKE`
- Regex: `~` (match), `!~` (not match)
- Null checks: `IS NULL`, `IS NOT NULL`
- List membership: `IN (...)`, `NOT IN (...)`
- Logical: `AND`, `OR`, `NOT`
- Attribute access: `attributes['key']`, `resource['key']`

## Tips for Agent Use

- Always use `--no-tui` when starting the server from an agent context
- Use `--format jsonl` for programmatic processing of query results
- Use `--format csv` for tabular data export
- Start the server in the background: `otel-cli server --no-tui &`
- Use `--since` to narrow down results to the relevant time window
- Combine `--service` and `--attribute` filters to find specific telemetry data
- Use `otel-cli sql` for complex queries with JOINs, projections, and advanced filtering
- Use `otel-cli clear` between test runs to reset state
