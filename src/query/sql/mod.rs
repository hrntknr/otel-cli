pub mod convert;
mod eval_logs;
mod eval_metrics;
mod eval_traces;
pub mod parser;

use crate::query::QueryResult;
use crate::store::Store;

pub use parser::{parse, SqlQuery};

pub fn execute(store: &Store, query: &SqlQuery) -> QueryResult {
    match query.table {
        crate::query::TargetTable::Traces => {
            QueryResult::Traces(eval_traces::eval_traces(store, query))
        }
        crate::query::TargetTable::Logs => QueryResult::Logs(eval_logs::eval_logs(store, query)),
        crate::query::TargetTable::Metrics => {
            QueryResult::Metrics(eval_metrics::eval_metrics(store, query))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::opentelemetry::proto::{
        common::v1::{any_value, AnyValue, KeyValue},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        metrics::v1::{
            metric, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics,
            ScopeMetrics,
        },
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
    };

    fn make_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
        }
    }

    fn make_resource(service_name: &str) -> Option<Resource> {
        Some(Resource {
            attributes: vec![make_kv("service.name", service_name)],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        })
    }

    fn setup_store() -> Store {
        let (mut store, _rx) = Store::new(100);

        // Insert traces
        store.insert_traces(vec![
            ResourceSpans {
                resource: make_resource("frontend"),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![0, 0, 0, 0, 0, 0, 0, 1],
                        trace_state: String::new(),
                        parent_span_id: vec![],
                        flags: 0,
                        name: "GET /api".to_string(),
                        kind: 2,
                        start_time_unix_nano: 1000,
                        end_time_unix_nano: 2000,
                        attributes: vec![make_kv("http.method", "GET")],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: Some(Status {
                            message: String::new(),
                            code: 0,
                        }),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            },
            ResourceSpans {
                resource: make_resource("backend"),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![2; 16],
                        span_id: vec![0, 0, 0, 0, 0, 0, 0, 2],
                        trace_state: String::new(),
                        parent_span_id: vec![],
                        flags: 0,
                        name: "POST /api".to_string(),
                        kind: 2,
                        start_time_unix_nano: 2000,
                        end_time_unix_nano: 5000,
                        attributes: vec![make_kv("http.method", "POST")],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: Some(Status {
                            message: String::new(),
                            code: 0,
                        }),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            },
        ]);

        // Insert logs
        store.insert_logs(vec![
            ResourceLogs {
                resource: make_resource("frontend"),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1000,
                        observed_time_unix_nano: 0,
                        severity_number: 9, // INFO
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "request started".to_string(),
                            )),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![],
                        span_id: vec![],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            },
            ResourceLogs {
                resource: make_resource("backend"),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 2000,
                        observed_time_unix_nano: 0,
                        severity_number: 17, // ERROR
                        severity_text: "ERROR".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("db error".to_string())),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![],
                        span_id: vec![],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            },
        ]);

        // Insert metrics
        store.insert_metrics(vec![ResourceMetrics {
            resource: make_resource("frontend"),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "http.duration".to_string(),
                    description: String::new(),
                    unit: String::new(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: 0,
                            time_unix_nano: 1000,
                            value: Some(number_data_point::Value::AsDouble(150.0)),
                            exemplars: vec![],
                            flags: 0,
                        }],
                    })),
                    metadata: vec![],
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }]);

        store
    }

    #[test]
    fn end_to_end_traces_query() {
        let store = setup_store();
        let query = parse("SELECT * FROM traces WHERE service_name = 'frontend'").unwrap();
        let result = execute(&store, &query);
        match result {
            QueryResult::Traces(traces) => assert_eq!(traces.len(), 1),
            _ => panic!("expected Traces"),
        }
    }

    #[test]
    fn end_to_end_logs_query() {
        let store = setup_store();
        let query = parse("SELECT * FROM logs WHERE severity >= 'ERROR'").unwrap();
        let result = execute(&store, &query);
        match result {
            QueryResult::Logs(logs) => assert_eq!(logs.len(), 1),
            _ => panic!("expected Logs"),
        }
    }

    #[test]
    fn end_to_end_metrics_query() {
        let store = setup_store();
        let query = parse("SELECT * FROM metrics WHERE metric_name = 'http.duration'").unwrap();
        let result = execute(&store, &query);
        match result {
            QueryResult::Metrics(metrics) => assert_eq!(metrics.len(), 1),
            _ => panic!("expected Metrics"),
        }
    }

    #[test]
    fn end_to_end_with_limit() {
        let store = setup_store();
        let query = parse("SELECT * FROM traces LIMIT 1").unwrap();
        let result = execute(&store, &query);
        match result {
            QueryResult::Traces(traces) => assert_eq!(traces.len(), 1),
            _ => panic!("expected Traces"),
        }
    }

    #[test]
    fn end_to_end_attribute_filter() {
        let store = setup_store();
        let query = parse("SELECT * FROM traces WHERE attributes['http.method'] = 'GET'").unwrap();
        let result = execute(&store, &query);
        match result {
            QueryResult::Traces(traces) => assert_eq!(traces.len(), 1),
            _ => panic!("expected Traces"),
        }
    }

    #[test]
    fn end_to_end_all_traces() {
        let store = setup_store();
        let query = parse("SELECT * FROM traces").unwrap();
        let result = execute(&store, &query);
        match result {
            QueryResult::Traces(traces) => assert_eq!(traces.len(), 2),
            _ => panic!("expected Traces"),
        }
    }

    #[test]
    fn end_to_end_all_logs() {
        let store = setup_store();
        let query = parse("SELECT * FROM logs").unwrap();
        let result = execute(&store, &query);
        match result {
            QueryResult::Logs(logs) => assert_eq!(logs.len(), 2),
            _ => panic!("expected Logs"),
        }
    }

    #[test]
    fn end_to_end_all_metrics() {
        let store = setup_store();
        let query = parse("SELECT * FROM metrics").unwrap();
        let result = execute(&store, &query);
        match result {
            QueryResult::Metrics(metrics) => assert_eq!(metrics.len(), 1),
            _ => panic!("expected Metrics"),
        }
    }
}
