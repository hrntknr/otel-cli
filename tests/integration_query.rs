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
use tokio::time::{timeout, Duration};
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
async fn test_sql_query_traces_with_service_filter() {
    let grpc_port = get_available_port();
    let query_port = get_available_port();
    let (_store, _shutdown) = start_servers(grpc_port, query_port).await;
    let addr = format!("http://127.0.0.1:{}", grpc_port);
    let query_addr = format!("http://127.0.0.1:{}", query_port);

    // Ingest traces from two services
    let mut trace_client = TraceServiceClient::connect(addr.clone()).await.unwrap();
    trace_client
        .export(ExportTraceServiceRequest {
            resource_spans: vec![
                ResourceSpans {
                    resource: make_resource("service-a"),
                    scope_spans: vec![ScopeSpans {
                        scope: None,
                        spans: vec![Span {
                            trace_id: vec![1; 16],
                            span_id: vec![1; 8],
                            name: "span-a".into(),
                            ..Default::default()
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                },
                ResourceSpans {
                    resource: make_resource("service-b"),
                    scope_spans: vec![ScopeSpans {
                        scope: None,
                        spans: vec![Span {
                            trace_id: vec![2; 16],
                            span_id: vec![2; 8],
                            name: "span-b".into(),
                            ..Default::default()
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                },
            ],
        })
        .await
        .unwrap();

    // Query filtering by service-a using SQL
    let mut query_client = QueryServiceClient::connect(query_addr).await.unwrap();
    let response = query_client
        .sql_query(SqlQueryRequest {
            query: "SELECT * FROM traces WHERE service_name = 'service-a'".into(),
        })
        .await
        .unwrap();

    let trace_groups = response.into_inner().trace_groups;
    assert_eq!(trace_groups.len(), 1);
    assert_eq!(
        trace_groups[0].resource_spans[0].scope_spans[0].spans[0].name,
        "span-a"
    );
}

#[tokio::test]
async fn test_sql_query_logs_with_severity_filter() {
    let grpc_port = get_available_port();
    let query_port = get_available_port();
    let (_store, _shutdown) = start_servers(grpc_port, query_port).await;
    let addr = format!("http://127.0.0.1:{}", grpc_port);
    let query_addr = format!("http://127.0.0.1:{}", query_port);

    // Ingest logs with different severities
    let mut logs_client = LogsServiceClient::connect(addr.clone()).await.unwrap();
    logs_client
        .export(ExportLogsServiceRequest {
            resource_logs: vec![
                ResourceLogs {
                    resource: make_resource("log-svc"),
                    scope_logs: vec![ScopeLogs {
                        scope: None,
                        log_records: vec![LogRecord {
                            severity_text: "ERROR".into(),
                            severity_number: 17,
                            ..Default::default()
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                },
                ResourceLogs {
                    resource: make_resource("log-svc"),
                    scope_logs: vec![ScopeLogs {
                        scope: None,
                        log_records: vec![LogRecord {
                            severity_text: "INFO".into(),
                            severity_number: 9,
                            ..Default::default()
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                },
            ],
        })
        .await
        .unwrap();

    // Query filtering by severity = ERROR using SQL
    let mut query_client = QueryServiceClient::connect(query_addr).await.unwrap();
    let response = query_client
        .sql_query(SqlQueryRequest {
            query: "SELECT * FROM logs WHERE severity = 'ERROR'".into(),
        })
        .await
        .unwrap();

    let logs = response.into_inner().resource_logs;
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].scope_logs[0].log_records[0].severity_text, "ERROR");
}

#[tokio::test]
async fn test_sql_query_logs_severity_ge() {
    let grpc_port = get_available_port();
    let query_port = get_available_port();
    let (_store, _shutdown) = start_servers(grpc_port, query_port).await;
    let addr = format!("http://127.0.0.1:{}", grpc_port);
    let query_addr = format!("http://127.0.0.1:{}", query_port);

    // Ingest logs: DEBUG(5), INFO(9), WARN(13), ERROR(17)
    let mut logs_client = LogsServiceClient::connect(addr.clone()).await.unwrap();
    let severities = [("DEBUG", 5), ("INFO", 9), ("WARN", 13), ("ERROR", 17)];
    for (text, num) in &severities {
        logs_client
            .export(ExportLogsServiceRequest {
                resource_logs: vec![ResourceLogs {
                    resource: make_resource("log-svc"),
                    scope_logs: vec![ScopeLogs {
                        scope: None,
                        log_records: vec![LogRecord {
                            severity_text: text.to_string(),
                            severity_number: *num,
                            ..Default::default()
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }],
            })
            .await
            .unwrap();
    }

    // Query with severity >= WARN using SQL
    let mut query_client = QueryServiceClient::connect(query_addr).await.unwrap();
    let response = query_client
        .sql_query(SqlQueryRequest {
            query: "SELECT * FROM logs WHERE severity >= 'WARN'".into(),
        })
        .await
        .unwrap();

    let logs = response.into_inner().resource_logs;
    assert_eq!(logs.len(), 2);
    let mut sev_numbers: Vec<i32> = logs
        .iter()
        .flat_map(|rl| {
            rl.scope_logs
                .iter()
                .flat_map(|sl| sl.log_records.iter().map(|lr| lr.severity_number))
        })
        .collect();
    sev_numbers.sort();
    assert_eq!(sev_numbers, vec![13, 17]);
}

#[tokio::test]
async fn test_sql_query_logs_with_service_name_filter() {
    let grpc_port = get_available_port();
    let query_port = get_available_port();
    let (_store, _shutdown) = start_servers(grpc_port, query_port).await;
    let addr = format!("http://127.0.0.1:{}", grpc_port);
    let query_addr = format!("http://127.0.0.1:{}", query_port);

    // Ingest logs from two services
    let mut logs_client = LogsServiceClient::connect(addr.clone()).await.unwrap();
    for svc in &["frontend", "backend", "frontend"] {
        logs_client
            .export(ExportLogsServiceRequest {
                resource_logs: vec![ResourceLogs {
                    resource: make_resource(svc),
                    scope_logs: vec![ScopeLogs {
                        scope: None,
                        log_records: vec![LogRecord {
                            severity_text: "INFO".into(),
                            severity_number: 9,
                            ..Default::default()
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }],
            })
            .await
            .unwrap();
    }

    // Query filtering by service_name using SQL
    let mut query_client = QueryServiceClient::connect(query_addr).await.unwrap();
    let response = query_client
        .sql_query(SqlQueryRequest {
            query: "SELECT * FROM logs WHERE service_name = 'frontend'".into(),
        })
        .await
        .unwrap();

    let logs = response.into_inner().resource_logs;
    assert_eq!(logs.len(), 2);
}

#[tokio::test]
async fn test_sql_query_metrics_with_name_filter() {
    let grpc_port = get_available_port();
    let query_port = get_available_port();
    let (_store, _shutdown) = start_servers(grpc_port, query_port).await;
    let addr = format!("http://127.0.0.1:{}", grpc_port);
    let query_addr = format!("http://127.0.0.1:{}", query_port);

    // Ingest metrics with different names
    let mut metrics_client = MetricsServiceClient::connect(addr.clone()).await.unwrap();
    metrics_client
        .export(ExportMetricsServiceRequest {
            resource_metrics: vec![
                ResourceMetrics {
                    resource: make_resource("metric-svc"),
                    scope_metrics: vec![ScopeMetrics {
                        scope: None,
                        metrics: vec![Metric {
                            name: "cpu_usage".into(),
                            description: String::new(),
                            unit: String::new(),
                            metadata: vec![],
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![NumberDataPoint::default()],
                            })),
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                },
                ResourceMetrics {
                    resource: make_resource("metric-svc"),
                    scope_metrics: vec![ScopeMetrics {
                        scope: None,
                        metrics: vec![Metric {
                            name: "memory_usage".into(),
                            description: String::new(),
                            unit: String::new(),
                            metadata: vec![],
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![NumberDataPoint::default()],
                            })),
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                },
            ],
        })
        .await
        .unwrap();

    // Query filtering by metric name using SQL
    let mut query_client = QueryServiceClient::connect(query_addr).await.unwrap();
    let response = query_client
        .sql_query(SqlQueryRequest {
            query: "SELECT * FROM metrics WHERE metric_name = 'cpu_usage'".into(),
        })
        .await
        .unwrap();

    let metrics = response.into_inner().resource_metrics;
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].scope_metrics[0].metrics[0].name, "cpu_usage");
}

#[tokio::test]
async fn test_follow_sql_traces() {
    let grpc_port = get_available_port();
    let query_port = get_available_port();
    let (_store, _shutdown) = start_servers(grpc_port, query_port).await;
    let addr = format!("http://127.0.0.1:{}", grpc_port);
    let query_addr = format!("http://127.0.0.1:{}", query_port);

    // Ingest initial span
    let mut trace_client = TraceServiceClient::connect(addr.clone()).await.unwrap();
    trace_client
        .export(ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: make_resource("svc-a"),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![1; 8],
                        name: "span-1".into(),
                        start_time_unix_nano: 100,
                        end_time_unix_nano: 200,
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        })
        .await
        .unwrap();

    // Start follow_sql for traces
    let mut query_client = QueryServiceClient::connect(query_addr.clone())
        .await
        .unwrap();
    let mut stream = query_client
        .follow_sql(SqlQueryRequest {
            query: "SELECT * FROM traces".into(),
        })
        .await
        .unwrap()
        .into_inner();

    // First message: initial batch
    let initial = timeout(Duration::from_secs(2), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(initial.trace_groups.len(), 1);
    assert_eq!(
        initial.trace_groups[0].resource_spans[0].scope_spans[0].spans[0].name,
        "span-1"
    );

    // Add a new trace
    trace_client
        .export(ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: make_resource("svc-a"),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![2; 16],
                        span_id: vec![2; 8],
                        name: "span-2".into(),
                        start_time_unix_nano: 200,
                        end_time_unix_nano: 300,
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        })
        .await
        .unwrap();

    // Delta message should contain the new trace
    let delta = timeout(Duration::from_secs(2), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!delta.trace_groups.is_empty());
}
