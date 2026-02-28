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

    let rows = response.into_inner().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(get_row_string(&rows[0], "span_name").unwrap(), "span-a");
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

    let rows = response.into_inner().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(get_row_string(&rows[0], "severity").unwrap(), "ERROR");
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

    let rows = response.into_inner().rows;
    assert_eq!(rows.len(), 2);
    let mut severities: Vec<String> = rows
        .iter()
        .filter_map(|r| get_row_string(r, "severity"))
        .collect();
    severities.sort();
    assert_eq!(severities, vec!["ERROR", "WARN"]);
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

    let rows = response.into_inner().rows;
    assert_eq!(rows.len(), 2);
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

    let rows = response.into_inner().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        get_row_string(&rows[0], "metric_name").unwrap(),
        "cpu_usage"
    );
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
    assert_eq!(initial.rows.len(), 1);
    assert_eq!(
        get_row_string(&initial.rows[0], "span_name").unwrap(),
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
    assert!(!delta.rows.is_empty());
}

#[tokio::test]
async fn test_sql_query_with_column_projection() {
    let grpc_port = get_available_port();
    let query_port = get_available_port();
    let (_store, _shutdown) = start_servers(grpc_port, query_port).await;
    let addr = format!("http://127.0.0.1:{}", grpc_port);
    let query_addr = format!("http://127.0.0.1:{}", query_port);

    // Ingest a trace
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
                        name: "test-span".into(),
                        kind: 2,
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        })
        .await
        .unwrap();

    // Query with specific columns
    let mut query_client = QueryServiceClient::connect(query_addr).await.unwrap();
    let response = query_client
        .sql_query(SqlQueryRequest {
            query: "SELECT span_name, service_name FROM traces".into(),
        })
        .await
        .unwrap();

    let rows = response.into_inner().rows;
    assert_eq!(rows.len(), 1);
    // Should have exactly 2 columns
    assert_eq!(rows[0].columns.len(), 2);
    assert_eq!(rows[0].columns[0].name, "span_name");
    assert_eq!(rows[0].columns[1].name, "service_name");
    assert_eq!(get_row_string(&rows[0], "span_name").unwrap(), "test-span");
    assert_eq!(get_row_string(&rows[0], "service_name").unwrap(), "svc-a");
}
