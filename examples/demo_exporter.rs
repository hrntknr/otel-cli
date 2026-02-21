use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use clap::Parser;
use rand::Rng;
use tonic::transport::Channel;

use otel_cli::proto::opentelemetry::proto::{
    collector::{
        logs::v1::{logs_service_client::LogsServiceClient, ExportLogsServiceRequest},
        metrics::v1::{
            metrics_service_client::MetricsServiceClient, ExportMetricsServiceRequest,
        },
        trace::v1::{trace_service_client::TraceServiceClient, ExportTraceServiceRequest},
    },
    common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
    metrics::v1::{
        metric, number_data_point, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, Sum,
    },
    resource::v1::Resource,
    trace::v1::{span::SpanKind, ResourceSpans, ScopeSpans, Span, Status},
};

#[derive(Parser)]
#[command(name = "demo_exporter", about = "Generate demo telemetry data")]
struct Args {
    /// Target endpoint
    #[arg(long, default_value = "http://localhost:4317")]
    endpoint: String,

    /// Send interval in milliseconds
    #[arg(long, default_value_t = 1000)]
    interval: u64,

    /// Send once and exit
    #[arg(long)]
    once: bool,
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn str_val(s: &str) -> Option<AnyValue> {
    Some(AnyValue {
        value: Some(any_value::Value::StringValue(s.into())),
    })
}

fn int_val(v: i64) -> Option<AnyValue> {
    Some(AnyValue {
        value: Some(any_value::Value::IntValue(v)),
    })
}

fn kv(key: &str, value: Option<AnyValue>) -> KeyValue {
    KeyValue {
        key: key.into(),
        value,
    }
}

fn resource(service_name: &str) -> Option<Resource> {
    Some(Resource {
        attributes: vec![kv("service.name", str_val(service_name))],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    })
}

fn scope() -> Option<InstrumentationScope> {
    Some(InstrumentationScope {
        name: "demo_exporter".into(),
        version: "0.1.0".into(),
        attributes: vec![],
        dropped_attributes_count: 0,
    })
}

struct SpanDef {
    service: &'static str,
    name: &'static str,
    kind: i32,
    /// Offset from trace start in ms
    offset_ms: u64,
    /// Duration in ms
    duration_ms: u64,
    /// Index of parent span in the list (None for root)
    parent_idx: Option<usize>,
    attributes: Vec<KeyValue>,
}

fn generate_traces(rng: &mut impl Rng) -> Vec<ResourceSpans> {
    let trace_id: Vec<u8> = (0..16).map(|_| rng.random()).collect();
    let span_ids: Vec<Vec<u8>> = (0..3).map(|_| (0..8).map(|_| rng.random()).collect()).collect();
    let base_time = now_nanos();

    let defs = [
        SpanDef {
            service: "frontend",
            name: "HTTP GET /users",
            kind: SpanKind::Server as i32,
            offset_ms: 0,
            duration_ms: 120,
            parent_idx: None,
            attributes: vec![
                kv("http.method", str_val("GET")),
                kv("http.route", str_val("/users")),
                kv("http.status_code", int_val(200)),
            ],
        },
        SpanDef {
            service: "api-gateway",
            name: "route /users",
            kind: SpanKind::Internal as i32,
            offset_ms: 5,
            duration_ms: 100,
            parent_idx: Some(0),
            attributes: vec![
                kv("rpc.system", str_val("grpc")),
                kv("rpc.service", str_val("UserService")),
            ],
        },
        SpanDef {
            service: "user-service",
            name: "SELECT users",
            kind: SpanKind::Client as i32,
            offset_ms: 15,
            duration_ms: 60,
            parent_idx: Some(1),
            attributes: vec![
                kv("db.system", str_val("postgresql")),
                kv("db.statement", str_val("SELECT * FROM users LIMIT 100")),
            ],
        },
    ];

    // Group spans by service → ResourceSpans
    let mut result: Vec<ResourceSpans> = Vec::new();
    for (i, def) in defs.iter().enumerate() {
        let start = base_time + def.offset_ms * 1_000_000;
        let end = start + def.duration_ms * 1_000_000;
        let parent_span_id = match def.parent_idx {
            Some(idx) => span_ids[idx].clone(),
            None => vec![],
        };

        let span = Span {
            trace_id: trace_id.clone(),
            span_id: span_ids[i].clone(),
            parent_span_id,
            name: def.name.into(),
            kind: def.kind,
            start_time_unix_nano: start,
            end_time_unix_nano: end,
            attributes: def.attributes.clone(),
            status: Some(Status {
                code: 1, // Ok
                message: String::new(),
            }),
            ..Default::default()
        };

        result.push(ResourceSpans {
            resource: resource(def.service),
            scope_spans: vec![ScopeSpans {
                scope: scope(),
                spans: vec![span],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        });
    }

    result
}

struct LogDef {
    service: &'static str,
    severity: SeverityNumber,
    body: &'static str,
}

const LOG_TEMPLATES: &[LogDef] = &[
    LogDef {
        service: "frontend",
        severity: SeverityNumber::Info,
        body: "Request completed successfully",
    },
    LogDef {
        service: "frontend",
        severity: SeverityNumber::Info,
        body: "Serving static assets from cache",
    },
    LogDef {
        service: "api-gateway",
        severity: SeverityNumber::Warn,
        body: "Connection pool nearing capacity (85%)",
    },
    LogDef {
        service: "api-gateway",
        severity: SeverityNumber::Info,
        body: "Rate limiter reset for client 10.0.0.5",
    },
    LogDef {
        service: "user-service",
        severity: SeverityNumber::Error,
        body: "Database query timeout after 5000ms",
    },
    LogDef {
        service: "user-service",
        severity: SeverityNumber::Info,
        body: "Cache hit for user profile id=42",
    },
    LogDef {
        service: "user-service",
        severity: SeverityNumber::Warn,
        body: "Slow query detected: SELECT users (>200ms)",
    },
    LogDef {
        service: "frontend",
        severity: SeverityNumber::Error,
        body: "Upstream service returned 503",
    },
];

fn severity_text(s: SeverityNumber) -> &'static str {
    match s {
        SeverityNumber::Info => "INFO",
        SeverityNumber::Warn => "WARN",
        SeverityNumber::Error => "ERROR",
        _ => "UNSPECIFIED",
    }
}

fn generate_logs(rng: &mut impl Rng) -> Vec<ResourceLogs> {
    let now = now_nanos();
    // Pick 2-4 random log entries
    let count = rng.random_range(2..=4);
    let mut result: Vec<ResourceLogs> = Vec::new();

    for _ in 0..count {
        let idx = rng.random_range(0..LOG_TEMPLATES.len());
        let def = &LOG_TEMPLATES[idx];

        let record = LogRecord {
            time_unix_nano: now,
            observed_time_unix_nano: now,
            severity_number: def.severity as i32,
            severity_text: severity_text(def.severity).into(),
            body: str_val(def.body),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![],
            span_id: vec![],
            event_name: String::new(),
        };

        result.push(ResourceLogs {
            resource: resource(def.service),
            scope_logs: vec![ScopeLogs {
                scope: scope(),
                log_records: vec![record],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        });
    }

    result
}

/// Persistent state for metrics that accumulate across sends.
struct MetricsState {
    request_count: i64,
    start_time_nanos: u64,
}

impl MetricsState {
    fn new() -> Self {
        Self {
            request_count: 0,
            start_time_nanos: now_nanos(),
        }
    }
}

fn generate_metrics(rng: &mut impl Rng, state: &mut MetricsState) -> Vec<ResourceMetrics> {
    let now = now_nanos();

    // http.request.count — monotonic Sum
    state.request_count += rng.random_range(1..=10);
    let request_count = Metric {
        name: "http.request.count".into(),
        description: "Total number of HTTP requests".into(),
        unit: "{request}".into(),
        metadata: vec![],
        data: Some(metric::Data::Sum(Sum {
            data_points: vec![NumberDataPoint {
                attributes: vec![kv("http.method", str_val("GET"))],
                start_time_unix_nano: state.start_time_nanos,
                time_unix_nano: now,
                value: Some(number_data_point::Value::AsInt(state.request_count)),
                exemplars: vec![],
                flags: 0,
            }],
            aggregation_temporality: 2, // CUMULATIVE
            is_monotonic: true,
        })),
    };

    // http.request.duration — Histogram
    let sample_count: u64 = rng.random_range(5..=20);
    let bounds = vec![5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0];
    let mut bucket_counts = vec![0u64; bounds.len() + 1];
    let mut sum = 0.0;
    let mut min = f64::MAX;
    let mut max = f64::MIN;
    for _ in 0..sample_count {
        let v: f64 = rng.random_range(1.0..500.0);
        sum += v;
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
        let bucket = bounds.iter().position(|&b| v <= b).unwrap_or(bounds.len());
        bucket_counts[bucket] += 1;
    }

    let request_duration = Metric {
        name: "http.request.duration".into(),
        description: "HTTP request duration".into(),
        unit: "ms".into(),
        metadata: vec![],
        data: Some(metric::Data::Histogram(Histogram {
            data_points: vec![HistogramDataPoint {
                attributes: vec![kv("http.method", str_val("GET"))],
                start_time_unix_nano: state.start_time_nanos,
                time_unix_nano: now,
                count: sample_count,
                sum: Some(sum),
                bucket_counts,
                explicit_bounds: bounds,
                exemplars: vec![],
                flags: 0,
                min: Some(min),
                max: Some(max),
            }],
            aggregation_temporality: 2, // CUMULATIVE
        })),
    };

    // system.cpu.usage — Gauge
    let cpu: f64 = rng.random_range(10.0..90.0);
    let cpu_usage = Metric {
        name: "system.cpu.usage".into(),
        description: "CPU usage percentage".into(),
        unit: "%".into(),
        metadata: vec![],
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                attributes: vec![kv("host.name", str_val("demo-host"))],
                start_time_unix_nano: state.start_time_nanos,
                time_unix_nano: now,
                value: Some(number_data_point::Value::AsDouble(cpu)),
                exemplars: vec![],
                flags: 0,
            }],
        })),
    };

    vec![ResourceMetrics {
        resource: resource("demo-service"),
        scope_metrics: vec![ScopeMetrics {
            scope: scope(),
            metrics: vec![request_count, request_duration, cpu_usage],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }]
}

async fn send_once(
    trace_client: &mut TraceServiceClient<Channel>,
    logs_client: &mut LogsServiceClient<Channel>,
    metrics_client: &mut MetricsServiceClient<Channel>,
    metrics_state: &mut MetricsState,
) -> Result<()> {
    let mut rng = rand::rng();

    let traces = generate_traces(&mut rng);
    let logs = generate_logs(&mut rng);
    let metrics = generate_metrics(&mut rng, metrics_state);

    let span_count: usize = traces
        .iter()
        .flat_map(|rs| &rs.scope_spans)
        .map(|ss| ss.spans.len())
        .sum();
    let log_count: usize = logs
        .iter()
        .flat_map(|rl| &rl.scope_logs)
        .map(|sl| sl.log_records.len())
        .sum();
    let metric_count: usize = metrics
        .iter()
        .flat_map(|rm| &rm.scope_metrics)
        .map(|sm| sm.metrics.len())
        .sum();

    trace_client
        .export(ExportTraceServiceRequest {
            resource_spans: traces,
        })
        .await?;
    logs_client
        .export(ExportLogsServiceRequest {
            resource_logs: logs,
        })
        .await?;
    metrics_client
        .export(ExportMetricsServiceRequest {
            resource_metrics: metrics,
        })
        .await?;

    println!(
        "Sent: {} spans, {} logs, {} metrics",
        span_count, log_count, metric_count
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let trace_client = TraceServiceClient::connect(args.endpoint.clone()).await?;
    let logs_client = LogsServiceClient::connect(args.endpoint.clone()).await?;
    let metrics_client = MetricsServiceClient::connect(args.endpoint.clone()).await?;

    // Clone into mutable bindings
    let mut trace_client = trace_client;
    let mut logs_client = logs_client;
    let mut metrics_client = metrics_client;
    let mut metrics_state = MetricsState::new();

    if args.once {
        send_once(
            &mut trace_client,
            &mut logs_client,
            &mut metrics_client,
            &mut metrics_state,
        )
        .await?;
    } else {
        println!(
            "Sending demo telemetry to {} every {}ms (Ctrl+C to stop)",
            args.endpoint, args.interval
        );
        loop {
            if let Err(e) = send_once(
                &mut trace_client,
                &mut logs_client,
                &mut metrics_client,
                &mut metrics_state,
            )
            .await
            {
                eprintln!("Error: {e}");
            }
            tokio::time::sleep(Duration::from_millis(args.interval)).await;
        }
    }

    Ok(())
}
