use otel_cli::proto::opentelemetry::proto::{
    collector::{
        logs::v1::{logs_service_client::LogsServiceClient, ExportLogsServiceRequest},
        metrics::v1::{metrics_service_client::MetricsServiceClient, ExportMetricsServiceRequest},
        trace::v1::{trace_service_client::TraceServiceClient, ExportTraceServiceRequest},
    },
    common::v1::{any_value, AnyValue, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{metric, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use otel_cli::proto::otelcli::query::v1::{
    query_service_client::QueryServiceClient, SqlQueryRequest,
};
use otel_cli::store;
use tokio_util::sync::CancellationToken;

fn get_row_string(row: &otel_cli::proto::otelcli::query::v1::Row, name: &str) -> Option<String> {
    row.columns.iter().find(|c| c.name == name).and_then(|c| {
        c.value.as_ref().map(|v| match &v.value {
            Some(any_value::Value::StringValue(s)) => s.clone(),
            Some(any_value::Value::IntValue(i)) => i.to_string(),
            Some(any_value::Value::DoubleValue(d)) => d.to_string(),
            _ => String::new(),
        })
    })
}

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

async fn start_servers(grpc_port: u16, query_port: u16) -> (store::SharedStore, CancellationToken) {
    let (shared_store, _rx) = store::new_shared(1000);
    let shutdown = CancellationToken::new();

    let grpc_addr: std::net::SocketAddr = format!("127.0.0.1:{}", grpc_port).parse().unwrap();
    let grpc_listener = tokio::net::TcpListener::bind(grpc_addr).await.unwrap();
    let store_clone = shared_store.clone();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        otel_cli::server::run_grpc_server(grpc_listener, store_clone, shutdown_clone)
            .await
            .unwrap();
    });

    let query_addr: std::net::SocketAddr = format!("127.0.0.1:{}", query_port).parse().unwrap();
    let query_listener = tokio::net::TcpListener::bind(query_addr).await.unwrap();
    let store_clone = shared_store.clone();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        otel_cli::server::run_query_server(query_listener, store_clone, shutdown_clone)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    (shared_store, shutdown)
}

#[tokio::test]
async fn test_e2e_trace_query() {
    let grpc_port = get_available_port();
    let query_port = get_available_port();
    let (_store, _shutdown) = start_servers(grpc_port, query_port).await;
    let addr = format!("http://127.0.0.1:{}", grpc_port);
    let query_addr = format!("http://127.0.0.1:{}", query_port);

    // Ingest traces
    let mut trace_client = TraceServiceClient::connect(addr.clone()).await.unwrap();
    trace_client
        .export(ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: make_resource("test-trace-svc"),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![0xab; 16],
                        span_id: vec![0xcd; 8],
                        name: "test-span".into(),
                        start_time_unix_nano: 1_000_000_000,
                        end_time_unix_nano: 2_000_000_000,
                        attributes: vec![KeyValue {
                            key: "http.method".into(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("GET".into())),
                            }),
                        }],
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        })
        .await
        .unwrap();

    // Query traces via SQL
    let mut query_client = QueryServiceClient::connect(query_addr).await.unwrap();
    let response = query_client
        .sql_query(SqlQueryRequest {
            query: "SELECT * FROM traces WHERE service_name = 'test-trace-svc'".into(),
        })
        .await
        .unwrap();

    let rows = response.into_inner().rows;
    assert!(!rows.is_empty(), "Expected non-empty trace results");
    assert_eq!(
        get_row_string(&rows[0], "span_name").as_deref(),
        Some("test-span")
    );
    let expected_trace_id = vec![0xab_u8; 16]
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    assert!(
        get_row_string(&rows[0], "trace_id")
            .as_ref()
            .map_or(false, |v| v.contains(&expected_trace_id)),
        "trace_id should contain the hex of [0xab; 16]"
    );
}

#[tokio::test]
async fn test_e2e_log_query() {
    let grpc_port = get_available_port();
    let query_port = get_available_port();
    let (_store, _shutdown) = start_servers(grpc_port, query_port).await;
    let addr = format!("http://127.0.0.1:{}", grpc_port);
    let query_addr = format!("http://127.0.0.1:{}", query_port);

    // Ingest logs
    let mut logs_client = LogsServiceClient::connect(addr.clone()).await.unwrap();
    logs_client
        .export(ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: make_resource("test-log-svc"),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        severity_text: "WARN".into(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("something happened".into())),
                        }),
                        attributes: vec![KeyValue {
                            key: "env".into(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("prod".into())),
                            }),
                        }],
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        })
        .await
        .unwrap();

    // Query logs via SQL
    let mut query_client = QueryServiceClient::connect(query_addr).await.unwrap();
    let response = query_client
        .sql_query(SqlQueryRequest {
            query: "SELECT * FROM logs WHERE service_name = 'test-log-svc'".into(),
        })
        .await
        .unwrap();

    let rows = response.into_inner().rows;
    assert!(!rows.is_empty(), "Expected non-empty log results");
    assert_eq!(
        get_row_string(&rows[0], "severity").as_deref(),
        Some("WARN")
    );
}

#[tokio::test]
async fn test_e2e_metric_query() {
    let grpc_port = get_available_port();
    let query_port = get_available_port();
    let (_store, _shutdown) = start_servers(grpc_port, query_port).await;
    let addr = format!("http://127.0.0.1:{}", grpc_port);
    let query_addr = format!("http://127.0.0.1:{}", query_port);

    // Ingest metrics
    let mut metrics_client = MetricsServiceClient::connect(addr.clone()).await.unwrap();
    metrics_client
        .export(ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: make_resource("test-metric-svc"),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "request_count".into(),
                        description: "Total requests".into(),
                        unit: "1".into(),
                        metadata: vec![],
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_700_000_000_000_000_000,
                                value: Some(
                                    otel_cli::proto::opentelemetry::proto::metrics::v1::number_data_point::Value::AsInt(42),
                                ),
                                ..Default::default()
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        })
        .await
        .unwrap();

    // Query metrics via SQL
    let mut query_client = QueryServiceClient::connect(query_addr).await.unwrap();
    let response = query_client
        .sql_query(SqlQueryRequest {
            query: "SELECT * FROM metrics WHERE metric_name = 'request_count'".into(),
        })
        .await
        .unwrap();

    let rows = response.into_inner().rows;
    assert!(!rows.is_empty(), "Expected non-empty metric results");
    assert_eq!(
        get_row_string(&rows[0], "metric_name").as_deref(),
        Some("request_count")
    );
}
