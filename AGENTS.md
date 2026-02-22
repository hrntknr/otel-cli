# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

otel-cli is a Rust CLI tool that acts as an in-memory OpenTelemetry (OTLP) server with querying and TUI visualization. It receives traces, logs, and metrics via standard OTLP gRPC/HTTP, stores them in memory with FIFO eviction, and provides a custom gRPC query API plus an interactive terminal UI.

## Build & Development Commands

```bash
cargo build                          # Build
cargo test                           # Run all tests
cargo test <test_name>               # Run a single test by name
cargo test --test e2e_client         # Run a single integration test file
cargo clippy                         # Lint
cargo fmt                            # Format
cargo run -- server                  # Start OTLP server (gRPC:4317, HTTP:4318, Query:4319)
cargo run -- server --tui            # Start with TUI
cargo run -- trace                   # Query traces
cargo run -- log                     # Query logs
cargo run -- metrics                 # Query metrics
```

## Architecture

**Core data flow**: OTLP ingestion (gRPC/HTTP) → `Store` (in-memory, Arc<RwLock>) → Query API (gRPC) / TUI (broadcast channels)

- **`src/store.rs`** — Central in-memory store. Traces are grouped by trace_id as `TraceGroup` with version tracking for delta streaming. Logs and metrics use `VecDeque` with FIFO eviction. Filtering (service name, attributes, time range, severity) happens at query time via `FilterCondition`/`FilterOperator`.
- **`src/server/`** — Three listeners: `otlp_grpc.rs` (standard OTLP TraceService/LogsService/MetricsService), `otlp_http.rs` (Axum `/v1/traces`, `/v1/logs`, `/v1/metrics`), `query_grpc.rs` (custom QueryService with streaming follow support).
- **`src/client/`** — CLI query commands. Each submodule (trace, log, metrics, clear) builds gRPC requests and formats output (Text/JSON/TOON). `mod.rs` contains shared utilities: `hex_encode`, `parse_time_spec`, `format_attributes_json`, `format_timestamp`.
- **`src/tui/`** — ratatui-based interactive UI with tabs for traces/logs/metrics. Uses broadcast channel events (`TracesAdded`, `LogsAdded`, etc.) for real-time updates. Dirty tracking for efficient refresh.
- **`src/cli.rs`** — clap derive command definitions (Server, Trace, Log, Metrics, Clear).
- **`proto/query.proto`** — Custom query/follow/clear gRPC API. Standard OTLP protos are in `proto/opentelemetry-proto/` (git submodule).
- **`build.rs`** — Compiles protobuf files via `tonic_prost_build`.

## Code Patterns

- Error handling: `anyhow::Result<T>` throughout.
- Shared state: `Arc<RwLock<Store>>` passed to all server handlers.
- Event notifications: `broadcast::Sender` for TUI updates.
- Trace IDs: stored as `Vec<u8>`, displayed as hex strings via `hex_encode()`/`hex_decode()`.
- Timestamps: nanoseconds since epoch internally, formatted as RFC3339 for display.
- Time specs in queries: relative (`30s`, `5m`, `1h`, `2d`) or RFC3339 absolute.

## Testing

- Unit tests are inline in modules (especially `store.rs` and `client/mod.rs`).
- Integration tests in `tests/`: `e2e_client.rs`, `integration_otlp_grpc.rs`, `integration_otlp_http.rs`, `integration_query.rs`.
- Tests use dynamic port binding (`get_available_port()` via OS port 0) and `#[tokio::test]`.
- Helper constructors like `make_resource_spans()`, `make_resource_logs()` build test protobuf data.
