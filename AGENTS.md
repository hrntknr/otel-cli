# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

otel-cli is a Rust CLI tool that acts as an in-memory OpenTelemetry (OTLP) server with querying and TUI visualization. It receives traces, logs, and metrics via standard OTLP gRPC/HTTP, stores them in memory with FIFO eviction, and provides a custom gRPC query API (including a SQL query engine) plus an interactive terminal UI.

## Build & Development Commands

```bash
cargo build                          # Build
cargo test                           # Run all tests
cargo test <test_name>               # Run a single test by name
cargo test --test e2e_client         # Run a single integration test file
cargo clippy                         # Lint
cargo fmt                            # Format
cargo run -- server                  # Start OTLP server with TUI (gRPC:4317, HTTP:4318, Query:4319)
cargo run -- server --no-tui         # Start headless server
cargo run -- view                    # Attach TUI to a running server (default: localhost:4319)
cargo run -- trace                   # Query traces
cargo run -- log                     # Query logs
cargo run -- metrics                 # Query metrics
cargo run -- sql "SELECT * FROM traces"  # Run SQL query
cargo run -- status                   # Check server status (trace/log/metric counts)
cargo run -- shutdown                 # Remotely shutdown a running server
cargo run -- skill-install            # Install Claude Code skill for current project
cargo run -- skill-install --global   # Install skill globally
```

## Architecture

**Core data flow**: OTLP ingestion (gRPC/HTTP) → `Store` (in-memory, Arc<RwLock>) → Query API (gRPC) / TUI (broadcast channels)

- **`src/store.rs`** — Central in-memory store. All signal types (traces, logs, metrics) use `VecDeque` with timestamp-sorted insertion and FIFO eviction. Trace grouping by trace_id is done client-side (TUI/CLI). Filtering happens at query time via the SQL query engine.
- **`src/server/`** — Three listeners: `otlp_grpc.rs` (standard OTLP TraceService/LogsService/MetricsService), `otlp_http.rs` (Axum `/v1/traces`, `/v1/logs`, `/v1/metrics`), `query_grpc.rs` (custom QueryService with streaming follow support and SQL query execution).
- **`src/client/`** — CLI query commands. Each submodule (trace, log, metrics, sql, clear) builds gRPC requests and formats output (Text/JSONL/CSV). `view.rs` connects to a running server's Follow streams and pipes data into a local Store to drive the TUI. `mod.rs` contains shared utilities: `hex_encode`, `parse_time_spec`, `format_attributes_json`, `format_timestamp`, `print_rows_jsonl`, `print_rows_csv`.
- **`src/query/`** — SQL query engine. `sql/parser.rs` parses SQL using `sqlparser` crate into an internal `SqlQuery` AST. `sql/eval_traces.rs`, `sql/eval_logs.rs`, `sql/eval_metrics.rs` evaluate WHERE clauses against stored data. `sql/mod.rs` orchestrates execution and projection of results. `sql/convert.rs` converts legacy CLI flags (--service, --attribute, etc.) into SQL strings.
- **`src/tui/`** — ratatui-based interactive UI with tabs for traces/logs/metrics. Traces have a timeline view, metrics have a chart view. Uses broadcast channel events (`TracesAdded`, `LogsAdded`, etc.) for real-time updates. Dirty tracking for efficient refresh.
- **`src/install.rs`** — `skill-install` subcommand logic. Embeds `skills/otel-cli/SKILL.md` via `include_str!` and writes it to the local project or `~/.claude/skills/` (with `--global`).
- **`src/cli.rs`** — clap derive command definitions (Server, View, Trace, Log, Metrics, Sql, Clear, Status, Shutdown, SkillInstall). Output formats: `Text`, `Jsonl`, `Csv`.
- **`proto/query.proto`** — Custom query/follow/clear/status/shutdown/SQL gRPC API. Standard OTLP protos are in `proto/opentelemetry-proto/` (git submodule).
- **`build.rs`** — Compiles protobuf files via `tonic_prost_build`.

## Code Patterns

- Error handling: `anyhow::Result<T>` throughout.
- Shared state: `Arc<RwLock<Store>>` passed to all server handlers.
- Event notifications: `broadcast::Sender` for TUI updates.
- Trace IDs: stored as `Vec<u8>`, displayed as hex strings via `hex_encode()`/`hex_decode()`.
- Timestamps: nanoseconds since epoch internally, formatted as RFC3339 for display.
- Time specs in queries: relative (`30s`, `5m`, `1h`, `2d`) or RFC3339 absolute.
- SQL query engine: uses `sqlparser` crate to parse SQL, then evaluates against the in-memory store. Supports WHERE with comparison, LIKE, regex (`~`/`!~`), IN, IS NULL, AND/OR/NOT. Supports column projection, ORDER BY, and LIMIT. Attribute access via bracket syntax: `attributes['key']`, `resource['key']`.
- CLI flag queries are internally converted to SQL via `src/query/sql/convert.rs`.

## Self-Instrumentation

otel-cli can instrument itself using the `--otlp-endpoint` flag (or `OTEL_EXPORTER_OTLP_ENDPOINT` env var). This sends otel-cli's own internal traces to another OTLP collector — useful for profiling and debugging otel-cli itself.

### Two-Server Setup

```bash
# Terminal 1: Collector server (receives otel-cli's own traces)
cargo run -- server --no-tui --grpc-addr 0.0.0.0:14317 --http-addr 0.0.0.0:14318 --query-addr 0.0.0.0:14319

# Terminal 2: Instrumented server (the one under test)
cargo run -- server --no-tui --otlp-endpoint http://localhost:14317
```

### Important Notes

- **Batch export delay**: The internal OpenTelemetry SDK uses batch export with a ~5 second schedule. After the instrumented server processes requests, wait ~5-10 seconds before querying the collector for the self-instrumentation traces.
- **Instrumented spans**: `otlp.grpc.export_traces`, `otlp.grpc.export_logs`, `otlp.grpc.export_metrics`, `otlp.http.export_traces`, `otlp.http.export_logs`, `otlp.http.export_metrics`, `store.insert_traces`, `store.insert_logs`, `store.insert_metrics`, `sql.parse`, `sql.execute`, `query.query_traces`, `query.query_logs`, `query.query_metrics`.
- **Lock contention analysis**: Spans include `busy_ns` and `idle_ns` attributes that measure RwLock acquisition time (`idle_ns` = time waiting for the lock, `busy_ns` = time holding the lock). High `idle_ns` values indicate lock contention.

### Analysis Example

```bash
# Query self-instrumentation traces from the collector server
cargo run -- trace --server http://localhost:14319 --format jsonl | jq '.duration_ns' | sort -n

# Use SQL for detailed analysis
cargo run -- sql --server http://localhost:14319 "SELECT span_name, duration_ns, attributes['busy_ns'], attributes['idle_ns'] FROM traces ORDER BY duration_ns DESC LIMIT 20"
```

## Testing

- Unit tests are inline in modules (especially `store.rs`, `client/mod.rs`, `query/sql/`).
- Integration tests in `tests/`: `e2e_client.rs`, `integration_otlp_grpc.rs`, `integration_otlp_http.rs`, `integration_query.rs`.
- Tests use dynamic port binding (`get_available_port()` via OS port 0) and `#[tokio::test]`.
- Helper constructors like `make_resource_spans()`, `make_resource_logs()` build test protobuf data.
