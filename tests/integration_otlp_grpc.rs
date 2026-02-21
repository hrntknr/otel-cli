use otel_cli::proto::opentelemetry::proto::{
    collector::{
        logs::v1::{
            logs_service_client::LogsServiceClient, ExportLogsServiceRequest,
        },
        metrics::v1::{
            metrics_service_client::MetricsServiceClient, ExportMetricsServiceRequest,
        },
        trace::v1::{
            trace_service_client::TraceServiceClient, ExportTraceServiceRequest,
        },
    },
    common::v1::{any_value, AnyValue, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        metric, Gauge, NumberDataPoint, Metric, ResourceMetrics, ScopeMetrics,
    },
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use otel_cli::proto::otelcli::query::v1::{
    query_service_client::QueryServiceClient, QueryLogsRequest, QueryMetricsRequest,
    QueryTracesRequest,
};
use otel_cli::store;
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

async fn start_grpc_server(port: u16) -> (store::SharedStore, CancellationToken) {
    let (shared_store, _rx) = store::new_shared(1000);
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let store_clone = shared_store.clone();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        otel_cli::server::run_grpc_server(addr, store_clone, shutdown_clone)
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    (shared_store, shutdown)
}

#[tokio::test]
async fn test_grpc_trace_ingest_and_query() {
    let port = get_available_port();
    let (_store, _shutdown) = start_grpc_server(port).await;
    let addr = format!("http://127.0.0.1:{}", port);

    // Send traces
    let mut trace_client = TraceServiceClient::connect(addr.clone()).await.unwrap();
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: make_resource("test-service"),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                    span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
                    name: "test-span".into(),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let response = trace_client.export(request).await.unwrap();
    assert!(response.into_inner().partial_success.is_none());

    // Query traces
    let mut query_client = QueryServiceClient::connect(addr).await.unwrap();
    let query_response = query_client
        .query_traces(QueryTracesRequest {
            service_name: "test-service".into(),
            trace_id: String::new(),
            attributes: Default::default(),
            limit: 10,
        })
        .await
        .unwrap();

    let spans = query_response.into_inner().resource_spans;
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].scope_spans[0].spans[0].name, "test-span");
}

#[tokio::test]
async fn test_grpc_logs_ingest_and_query() {
    let port = get_available_port();
    let (_store, _shutdown) = start_grpc_server(port).await;
    let addr = format!("http://127.0.0.1:{}", port);

    // Send logs
    let mut logs_client = LogsServiceClient::connect(addr.clone()).await.unwrap();
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: make_resource("log-service"),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    severity_text: "ERROR".into(),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let response = logs_client.export(request).await.unwrap();
    assert!(response.into_inner().partial_success.is_none());

    // Query logs
    let mut query_client = QueryServiceClient::connect(addr).await.unwrap();
    let query_response = query_client
        .query_logs(QueryLogsRequest {
            service_name: "log-service".into(),
            severity: String::new(),
            attributes: Default::default(),
            limit: 10,
        })
        .await
        .unwrap();

    let logs = query_response.into_inner().resource_logs;
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].scope_logs[0].log_records[0].severity_text, "ERROR");
}

#[tokio::test]
async fn test_grpc_metrics_ingest_and_query() {
    let port = get_available_port();
    let (_store, _shutdown) = start_grpc_server(port).await;
    let addr = format!("http://127.0.0.1:{}", port);

    // Send metrics
    let mut metrics_client = MetricsServiceClient::connect(addr.clone()).await.unwrap();
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: make_resource("metric-service"),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "cpu_usage".into(),
                    description: "CPU usage".into(),
                    unit: "percent".into(),
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
    let response = metrics_client.export(request).await.unwrap();
    assert!(response.into_inner().partial_success.is_none());

    // Query metrics
    let mut query_client = QueryServiceClient::connect(addr).await.unwrap();
    let query_response = query_client
        .query_metrics(QueryMetricsRequest {
            service_name: "metric-service".into(),
            metric_name: String::new(),
            limit: 10,
        })
        .await
        .unwrap();

    let metrics = query_response.into_inner().resource_metrics;
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].scope_metrics[0].metrics[0].name, "cpu_usage");
}
