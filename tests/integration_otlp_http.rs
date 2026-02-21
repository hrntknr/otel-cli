use otel_cli::proto::opentelemetry::proto::{
    collector::{
        logs::v1::ExportLogsServiceRequest,
        metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{any_value, AnyValue, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{metric, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use otel_cli::store;
use prost::Message;
use tokio_util::sync::CancellationToken;

fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn make_resource(service_name: &str) -> Option<Resource> {
    Some(Resource {
        attributes: vec![KeyValue {
            key: "service.name".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(service_name.into())),
            }),
        }],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    })
}

async fn start_http_server(port: u16) -> (store::SharedStore, CancellationToken) {
    let (shared_store, _rx) = store::new_shared(1000);
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let store_clone = shared_store.clone();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        otel_cli::server::run_http_server(listener, store_clone, shutdown_clone)
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    (shared_store, shutdown)
}

#[tokio::test]
async fn test_http_trace_ingest() {
    let port = get_available_port();
    let (_store, _shutdown) = start_http_server(port).await;

    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: make_resource("http-trace-svc"),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![0; 16],
                    span_id: vec![1; 8],
                    name: "http-span".into(),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{}/v1/traces", port))
        .header("Content-Type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let content_type = response.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(content_type, "application/x-protobuf");
}

#[tokio::test]
async fn test_http_logs_ingest() {
    let port = get_available_port();
    let (_store, _shutdown) = start_http_server(port).await;

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: make_resource("http-log-svc"),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    severity_text: "WARN".into(),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{}/v1/logs", port))
        .header("Content-Type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_http_metrics_ingest() {
    let port = get_available_port();
    let (_store, _shutdown) = start_http_server(port).await;

    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: make_resource("http-metric-svc"),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "memory_usage".into(),
                    description: "Memory usage".into(),
                    unit: "bytes".into(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            ..Default::default()
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{}/v1/metrics", port))
        .header("Content-Type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_http_invalid_body() {
    let port = get_available_port();
    let (_store, _shutdown) = start_http_server(port).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{}/v1/traces", port))
        .header("Content-Type", "application/x-protobuf")
        .body(b"this is not valid protobuf".to_vec())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400);
}
