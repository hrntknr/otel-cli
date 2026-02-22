# otel-cli

An in-memory OpenTelemetry (OTLP) server with querying and interactive TUI visualization, built in Rust.

Receive traces, logs, and metrics via standard OTLP protocols, store them in-memory, and inspect them interactively — no external infrastructure required.

## Features

- **OTLP Ingestion** — Accepts traces, logs, and metrics via gRPC (port 4317) and HTTP (port 4318)
- **Query API** — Custom gRPC query service (port 4319) with filtering and streaming
- **Interactive TUI** — Real-time terminal UI with tabs for traces, logs, and metrics
- **Follow Mode** — Stream new data in real-time with delta updates
- **Flexible Output** — Text, JSON, and TOON output formats
- **Filtering** — Filter by service name, severity, trace ID, attributes, and time range
- **In-Memory Store** — FIFO eviction with configurable capacity (default: 1000 items)

## Installation

### From GitHub Releases

Pre-built binaries are available on the [Releases](https://github.com/hrntknr/otel-cli/releases) page for Linux (x86_64), macOS (ARM), and Windows.

```bash
# Example: Linux x86_64
curl -L https://github.com/hrntknr/otel-cli/releases/latest/download/otel-cli-x86_64-unknown-linux-gnu -o otel-cli
chmod +x otel-cli
sudo mv otel-cli /usr/local/bin/
```

### From source

```bash
cargo install --path .
```

## Getting Started

1. Start the server:

```bash
otel-cli server
```

This launches an interactive TUI where you can inspect traces, logs, and metrics in real-time.

2. Configure your application's OTLP exporter to send to `localhost:4317` (gRPC) or `localhost:4318` (HTTP).

3. As data arrives, it appears live in the TUI.

## Usage

### Start the server

```bash
# With interactive TUI (default)
otel-cli server

# Headless mode
otel-cli server --no-tui
```

### Query traces

```bash
# List recent traces
otel-cli trace

# Filter by service name
otel-cli trace --service myapp

# Follow new traces in real-time
otel-cli trace -f

# Filter by time range
otel-cli trace --since 5m --format json
```

### Query logs

```bash
# List recent logs
otel-cli log

# Filter by severity (shows this level and above)
otel-cli log --severity ERROR

# Follow logs in real-time
otel-cli log -f --service myapp
```

### Query metrics

```bash
# List recent metrics
otel-cli metrics

# Filter by metric name
otel-cli metrics --name http_requests_total

# Follow metrics in real-time
otel-cli metrics -f --format json
```

### Clear data

```bash
# Clear all data
otel-cli clear --traces --logs --metrics

# Clear only traces
otel-cli clear --traces
```

### Common options

| Option | Description |
|---|---|
| `--server <ADDR>` | Query server address (default: `http://localhost:4319`) |
| `--service <NAME>` | Filter by service name |
| `--attribute <KEY=VALUE>` | Filter by attribute (repeatable) |
| `--limit <N>` | Maximum results (default: 100) |
| `--format <FORMAT>` | Output format: `text`, `json`, `toon` |
| `-f, --follow` | Follow new data in real-time |
| `--since <SPEC>` | Time range start (`30s`, `5m`, `1h`, `2d`, or RFC3339) |
| `--until <SPEC>` | Time range end (same format) |

## Architecture

```
OTLP Ingestion (gRPC:4317 / HTTP:4318)
        │
    Store (in-memory)
        │
   ┌────┴────┐
   │         │
Query API   TUI
(gRPC:4319)
```

## Development

```bash
# Build
cargo build

# Run tests
cargo test

# Run with example exporter
cargo run -- server --no-tui &
cargo run --example demo_exporter
```

## License

MIT
