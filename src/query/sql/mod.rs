pub mod convert;
mod eval_logs;
mod eval_metrics;
mod eval_traces;
pub mod parser;

use crate::client::{extract_any_value_string, get_service_name};
use crate::proto::opentelemetry::proto::common::v1::KeyValue;
use crate::proto::opentelemetry::proto::logs::v1::ResourceLogs;
use crate::proto::opentelemetry::proto::metrics::v1::{metric, number_data_point, ResourceMetrics};
use crate::query::{QueryResult, Row, RowValue};
use crate::store::{Store, TraceGroup};

use eval_traces::FieldValue;
pub use parser::{parse, Projection, SqlQuery};

pub fn execute(store: &Store, query: &SqlQuery) -> Vec<Row> {
    let result = eval(store, query);
    project(&result, &query.projection)
}

pub fn eval(store: &Store, query: &SqlQuery) -> QueryResult {
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

pub fn project(result: &QueryResult, projection: &Projection) -> Vec<Row> {
    match result {
        QueryResult::Traces(groups) => project_traces(groups, projection),
        QueryResult::Logs(logs) => project_logs(logs, projection),
        QueryResult::Metrics(metrics) => project_metrics(metrics, projection),
    }
}

// --- Default columns ---

const DEFAULT_TRACE_COLUMNS: &[&str] = &[
    "trace_id",
    "span_id",
    "parent_span_id",
    "service_name",
    "span_name",
    "kind",
    "status_code",
    "start_time",
    "end_time",
    "duration_ns",
    "resource",
    "attributes",
];

const DEFAULT_LOG_COLUMNS: &[&str] = &[
    "timestamp",
    "severity",
    "severity_number",
    "body",
    "service_name",
    "resource",
    "attributes",
];

const DEFAULT_METRIC_COLUMNS: &[&str] = &[
    "timestamp",
    "metric_name",
    "type",
    "value",
    "count",
    "sum",
    "service_name",
    "resource",
    "attributes",
];

fn resolve_columns(projection: &Projection, defaults: &[&str]) -> Vec<parser::ColumnRef> {
    match projection {
        Projection::All => defaults
            .iter()
            .map(|s| parser::ColumnRef::Named(s.to_string()))
            .collect(),
        Projection::Columns(cols) => cols.clone(),
    }
}

// --- RowValue conversion helpers ---

fn field_value_to_row_value(fv: FieldValue) -> RowValue {
    match fv {
        FieldValue::String(s) => RowValue::String(s),
        FieldValue::Number(n) => {
            if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                RowValue::Int(n as i64)
            } else {
                RowValue::Double(n)
            }
        }
        FieldValue::Null => RowValue::Null,
    }
}

// --- Traces projection ---

pub fn project_traces(groups: &[TraceGroup], projection: &Projection) -> Vec<Row> {
    let columns = resolve_columns(projection, DEFAULT_TRACE_COLUMNS);
    let mut rows = Vec::new();
    for group in groups {
        for rs in &group.resource_spans {
            let resource = &rs.resource;
            let resource_attrs = resource
                .as_ref()
                .map(|r| r.attributes.clone())
                .unwrap_or_default();
            for ss in &rs.scope_spans {
                for span in &ss.spans {
                    let mut row = Vec::new();
                    for col in &columns {
                        let (name, value) =
                            resolve_trace_column(span, resource, &resource_attrs, col);
                        row.push((name, value));
                    }
                    rows.push(row);
                }
            }
        }
    }
    rows
}

fn resolve_trace_column(
    span: &crate::proto::opentelemetry::proto::trace::v1::Span,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    resource_attrs: &[KeyValue],
    column: &parser::ColumnRef,
) -> (String, RowValue) {
    match column {
        parser::ColumnRef::Named(name) => {
            let value = match name.as_str() {
                "resource" => RowValue::KeyValueList(resource_attrs.to_vec()),
                "attributes" => RowValue::KeyValueList(span.attributes.clone()),
                _ => {
                    let fv = eval_traces::resolve_span_column(span, resource, column);
                    field_value_to_row_value(fv)
                }
            };
            (name.clone(), value)
        }
        parser::ColumnRef::BracketAccess(base, key) => {
            let fv = eval_traces::resolve_span_column(span, resource, column);
            (format!("{}['{}']", base, key), field_value_to_row_value(fv))
        }
    }
}

// --- Logs projection ---

pub fn project_logs(logs: &[ResourceLogs], projection: &Projection) -> Vec<Row> {
    let columns = resolve_columns(projection, DEFAULT_LOG_COLUMNS);
    let mut rows = Vec::new();
    for rl in logs {
        let resource = &rl.resource;
        let resource_attrs = resource
            .as_ref()
            .map(|r| r.attributes.clone())
            .unwrap_or_default();
        for sl in &rl.scope_logs {
            for lr in &sl.log_records {
                let mut row = Vec::new();
                for col in &columns {
                    let (name, value) =
                        resolve_log_column_for_projection(lr, resource, &resource_attrs, col);
                    row.push((name, value));
                }
                rows.push(row);
            }
        }
    }
    rows
}

fn resolve_log_column_for_projection(
    lr: &crate::proto::opentelemetry::proto::logs::v1::LogRecord,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    resource_attrs: &[KeyValue],
    column: &parser::ColumnRef,
) -> (String, RowValue) {
    match column {
        parser::ColumnRef::Named(name) => {
            let value = match name.as_str() {
                "resource" => RowValue::KeyValueList(resource_attrs.to_vec()),
                "attributes" => RowValue::KeyValueList(lr.attributes.clone()),
                "timestamp" => RowValue::String(crate::client::format_timestamp(lr.time_unix_nano)),
                "severity" => RowValue::String(lr.severity_text.clone()),
                "severity_number" => RowValue::Int(lr.severity_number as i64),
                "body" => match &lr.body {
                    Some(v) => RowValue::String(extract_any_value_string(v)),
                    None => RowValue::Null,
                },
                "service_name" => RowValue::String(get_service_name(resource)),
                _ => RowValue::Null,
            };
            (name.clone(), value)
        }
        parser::ColumnRef::BracketAccess(base, key) => {
            let fv = eval_logs::resolve_log_column_pub(lr, resource, column);
            (format!("{}['{}']", base, key), field_value_to_row_value(fv))
        }
    }
}

// --- Metrics projection ---

pub fn project_metrics(metrics: &[ResourceMetrics], projection: &Projection) -> Vec<Row> {
    let columns = resolve_columns(projection, DEFAULT_METRIC_COLUMNS);
    let mut rows = Vec::new();
    for rm in metrics {
        let resource = &rm.resource;
        let resource_attrs = resource
            .as_ref()
            .map(|r| r.attributes.clone())
            .unwrap_or_default();
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                let metric_type = metric_type_name(m);
                collect_metric_data_points(
                    m,
                    resource,
                    &resource_attrs,
                    &metric_type,
                    &columns,
                    &mut rows,
                );
            }
        }
    }
    rows
}

fn metric_type_name(metric: &crate::proto::opentelemetry::proto::metrics::v1::Metric) -> String {
    match &metric.data {
        Some(metric::Data::Gauge(_)) => "Gauge".to_string(),
        Some(metric::Data::Sum(_)) => "Sum".to_string(),
        Some(metric::Data::Histogram(_)) => "Histogram".to_string(),
        Some(metric::Data::ExponentialHistogram(_)) => "ExponentialHistogram".to_string(),
        Some(metric::Data::Summary(_)) => "Summary".to_string(),
        None => "Unknown".to_string(),
    }
}

fn collect_metric_data_points(
    m: &crate::proto::opentelemetry::proto::metrics::v1::Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    resource_attrs: &[KeyValue],
    metric_type: &str,
    columns: &[parser::ColumnRef],
    rows: &mut Vec<Row>,
) {
    match &m.data {
        Some(metric::Data::Gauge(g)) => {
            for dp in &g.data_points {
                rows.push(build_number_dp_row(
                    dp,
                    m,
                    resource,
                    resource_attrs,
                    metric_type,
                    columns,
                ));
            }
        }
        Some(metric::Data::Sum(s)) => {
            for dp in &s.data_points {
                rows.push(build_number_dp_row(
                    dp,
                    m,
                    resource,
                    resource_attrs,
                    metric_type,
                    columns,
                ));
            }
        }
        Some(metric::Data::Histogram(h)) => {
            for dp in &h.data_points {
                rows.push(build_histogram_dp_row(
                    dp,
                    m,
                    resource,
                    resource_attrs,
                    metric_type,
                    columns,
                ));
            }
        }
        Some(metric::Data::ExponentialHistogram(eh)) => {
            for dp in &eh.data_points {
                rows.push(build_exp_histogram_dp_row(
                    dp,
                    m,
                    resource,
                    resource_attrs,
                    metric_type,
                    columns,
                ));
            }
        }
        Some(metric::Data::Summary(s)) => {
            for dp in &s.data_points {
                rows.push(build_summary_dp_row(
                    dp,
                    m,
                    resource,
                    resource_attrs,
                    metric_type,
                    columns,
                ));
            }
        }
        None => {
            // No data points, emit a single row with null data values
            let row = columns
                .iter()
                .map(|col| {
                    resolve_metric_column_generic(
                        col,
                        resource,
                        resource_attrs,
                        m,
                        metric_type,
                        None,
                        None,
                        None,
                        &[],
                    )
                })
                .collect();
            rows.push(row);
        }
    }
}

fn resolve_metric_column_generic(
    col: &parser::ColumnRef,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    resource_attrs: &[KeyValue],
    m: &crate::proto::opentelemetry::proto::metrics::v1::Metric,
    metric_type: &str,
    timestamp: Option<u64>,
    value: Option<RowValue>,
    count_sum: Option<(Option<u64>, Option<f64>)>,
    dp_attrs: &[KeyValue],
) -> (String, RowValue) {
    match col {
        parser::ColumnRef::Named(name) => {
            let rv = match name.as_str() {
                "resource" => RowValue::KeyValueList(resource_attrs.to_vec()),
                "attributes" => RowValue::KeyValueList(dp_attrs.to_vec()),
                "timestamp" => match timestamp {
                    Some(ts) => RowValue::String(crate::client::format_timestamp(ts)),
                    None => RowValue::Null,
                },
                "metric_name" => RowValue::String(m.name.clone()),
                "type" => RowValue::String(metric_type.to_string()),
                "value" => value.clone().unwrap_or(RowValue::Null),
                "count" => match count_sum {
                    Some((Some(c), _)) => RowValue::Int(c as i64),
                    _ => RowValue::Null,
                },
                "sum" => match count_sum {
                    Some((_, Some(s))) => RowValue::Double(s),
                    _ => RowValue::Null,
                },
                "service_name" => RowValue::String(get_service_name(resource)),
                _ => RowValue::Null,
            };
            (name.clone(), rv)
        }
        parser::ColumnRef::BracketAccess(base, key) => {
            let fv = match base.as_str() {
                "attributes" => eval_traces::lookup_attribute(dp_attrs, key),
                "resource" => {
                    let attrs = resource
                        .as_ref()
                        .map(|r| r.attributes.as_slice())
                        .unwrap_or_default();
                    eval_traces::lookup_attribute(attrs, key)
                }
                _ => FieldValue::Null,
            };
            (format!("{}['{}']", base, key), field_value_to_row_value(fv))
        }
    }
}

fn build_number_dp_row(
    dp: &crate::proto::opentelemetry::proto::metrics::v1::NumberDataPoint,
    m: &crate::proto::opentelemetry::proto::metrics::v1::Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    resource_attrs: &[KeyValue],
    metric_type: &str,
    columns: &[parser::ColumnRef],
) -> Row {
    let value = match &dp.value {
        Some(number_data_point::Value::AsDouble(d)) => Some(RowValue::Double(*d)),
        Some(number_data_point::Value::AsInt(i)) => Some(RowValue::Int(*i)),
        None => None,
    };
    columns
        .iter()
        .map(|col| {
            resolve_metric_column_generic(
                col,
                resource,
                resource_attrs,
                m,
                metric_type,
                Some(dp.time_unix_nano),
                value.clone(),
                None,
                &dp.attributes,
            )
        })
        .collect()
}

fn build_histogram_dp_row(
    dp: &crate::proto::opentelemetry::proto::metrics::v1::HistogramDataPoint,
    m: &crate::proto::opentelemetry::proto::metrics::v1::Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    resource_attrs: &[KeyValue],
    metric_type: &str,
    columns: &[parser::ColumnRef],
) -> Row {
    columns
        .iter()
        .map(|col| {
            resolve_metric_column_generic(
                col,
                resource,
                resource_attrs,
                m,
                metric_type,
                Some(dp.time_unix_nano),
                None,
                Some((Some(dp.count), dp.sum)),
                &dp.attributes,
            )
        })
        .collect()
}

fn build_exp_histogram_dp_row(
    dp: &crate::proto::opentelemetry::proto::metrics::v1::ExponentialHistogramDataPoint,
    m: &crate::proto::opentelemetry::proto::metrics::v1::Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    resource_attrs: &[KeyValue],
    metric_type: &str,
    columns: &[parser::ColumnRef],
) -> Row {
    columns
        .iter()
        .map(|col| {
            resolve_metric_column_generic(
                col,
                resource,
                resource_attrs,
                m,
                metric_type,
                Some(dp.time_unix_nano),
                None,
                Some((Some(dp.count), dp.sum)),
                &dp.attributes,
            )
        })
        .collect()
}

fn build_summary_dp_row(
    dp: &crate::proto::opentelemetry::proto::metrics::v1::SummaryDataPoint,
    m: &crate::proto::opentelemetry::proto::metrics::v1::Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    resource_attrs: &[KeyValue],
    metric_type: &str,
    columns: &[parser::ColumnRef],
) -> Row {
    columns
        .iter()
        .map(|col| {
            resolve_metric_column_generic(
                col,
                resource,
                resource_attrs,
                m,
                metric_type,
                Some(dp.time_unix_nano),
                None,
                Some((Some(dp.count), Some(dp.sum))),
                &dp.attributes,
            )
        })
        .collect()
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
        assert_eq!(result.len(), 1);
        // Verify row has expected columns
        let row = &result[0];
        assert!(row.iter().any(|(name, _)| name == "trace_id"));
        assert!(row.iter().any(|(name, _)| name == "span_name"));
        assert!(row.iter().any(|(name, _)| name == "resource"));
        assert!(row.iter().any(|(name, _)| name == "attributes"));
    }

    #[test]
    fn end_to_end_logs_query() {
        let store = setup_store();
        let query = parse("SELECT * FROM logs WHERE severity >= 'ERROR'").unwrap();
        let result = execute(&store, &query);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn end_to_end_metrics_query() {
        let store = setup_store();
        let query = parse("SELECT * FROM metrics WHERE metric_name = 'http.duration'").unwrap();
        let result = execute(&store, &query);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn end_to_end_with_limit() {
        let store = setup_store();
        let query = parse("SELECT * FROM traces LIMIT 1").unwrap();
        let result = execute(&store, &query);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn end_to_end_attribute_filter() {
        let store = setup_store();
        let query = parse("SELECT * FROM traces WHERE attributes['http.method'] = 'GET'").unwrap();
        let result = execute(&store, &query);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn end_to_end_all_traces() {
        let store = setup_store();
        let query = parse("SELECT * FROM traces").unwrap();
        let result = execute(&store, &query);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn end_to_end_all_logs() {
        let store = setup_store();
        let query = parse("SELECT * FROM logs").unwrap();
        let result = execute(&store, &query);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn end_to_end_all_metrics() {
        let store = setup_store();
        let query = parse("SELECT * FROM metrics").unwrap();
        let result = execute(&store, &query);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn projection_specific_columns_traces() {
        let store = setup_store();
        let query = parse("SELECT span_name, service_name FROM traces").unwrap();
        let result = execute(&store, &query);
        assert_eq!(result.len(), 2);
        for row in &result {
            assert_eq!(row.len(), 2);
            assert_eq!(row[0].0, "span_name");
            assert_eq!(row[1].0, "service_name");
        }
    }

    #[test]
    fn projection_specific_columns_logs() {
        let store = setup_store();
        let query = parse("SELECT timestamp, severity FROM logs").unwrap();
        let result = execute(&store, &query);
        assert_eq!(result.len(), 2);
        for row in &result {
            assert_eq!(row.len(), 2);
            assert_eq!(row[0].0, "timestamp");
            assert_eq!(row[1].0, "severity");
        }
    }

    #[test]
    fn projection_resource_column() {
        let store = setup_store();
        let query = parse("SELECT span_name, resource FROM traces WHERE service_name = 'frontend'")
            .unwrap();
        let result = execute(&store, &query);
        assert_eq!(result.len(), 1);
        let row = &result[0];
        assert_eq!(row[0].0, "span_name");
        assert_eq!(row[1].0, "resource");
        match &row[1].1 {
            RowValue::KeyValueList(kvs) => {
                assert!(kvs.iter().any(|kv| kv.key == "service.name"));
            }
            _ => panic!("expected KeyValueList for resource"),
        }
    }
}
