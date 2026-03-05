pub mod convert;

use datafusion::arrow::array::{
    Array, AsArray, Float64Array, Int32Array, MapArray, StringArray, UInt64Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use tracing::instrument;

use crate::proto::opentelemetry::proto::common::v1::{any_value, AnyValue, KeyValue, KeyValueList};
use crate::proto::otelcli::query::v1::{ColumnValue, Row as ProtoRow};

#[instrument(name = "sql.execute", skip_all, fields(db.statement = sql))]
pub async fn execute(ctx: &SessionContext, sql: &str) -> Result<Vec<ProtoRow>, String> {
    let batches = crate::query::datafusion_ctx::execute_sql(ctx, sql).await?;
    Ok(record_batches_to_proto_rows(&batches))
}

fn record_batches_to_proto_rows(batches: &[RecordBatch]) -> Vec<ProtoRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let columns: Vec<ColumnValue> = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(col_idx, field)| {
                    let col = batch.column(col_idx);
                    ColumnValue {
                        name: field.name().clone(),
                        value: array_value_to_any_value(col.as_ref(), row_idx),
                    }
                })
                .collect();
            rows.push(ProtoRow { columns });
        }
    }
    rows
}

fn array_value_to_any_value(array: &dyn Array, idx: usize) -> Option<AnyValue> {
    if array.is_null(idx) {
        return None;
    }
    let value = match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            any_value::Value::StringValue(arr.value(idx).to_string())
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            any_value::Value::IntValue(arr.value(idx) as i64)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            any_value::Value::IntValue(arr.value(idx) as i64)
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap();
            any_value::Value::IntValue(arr.value(idx))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            any_value::Value::DoubleValue(arr.value(idx))
        }
        DataType::Map(_, _) => {
            let map_arr = array.as_any().downcast_ref::<MapArray>().unwrap();
            map_to_kvlist_value(map_arr, idx)
        }
        _ => {
            // Fallback: use Arrow display formatting
            use datafusion::arrow::util::display::ArrayFormatter;
            match ArrayFormatter::try_new(array, &Default::default()) {
                Ok(f) => any_value::Value::StringValue(format!("{}", f.value(idx))),
                Err(_) => return None,
            }
        }
    };
    Some(AnyValue { value: Some(value) })
}

fn map_to_kvlist_value(map_arr: &MapArray, idx: usize) -> any_value::Value {
    let offsets = map_arr.offsets();
    let start = offsets[idx] as usize;
    let end = offsets[idx + 1] as usize;

    let keys = map_arr.keys().as_string::<i32>();
    let values = map_arr.values().as_string::<i32>();

    let mut kvs = Vec::with_capacity(end - start);
    for i in start..end {
        let key = keys.value(i).to_string();
        let val = if values.is_null(i) {
            None
        } else {
            Some(AnyValue {
                value: Some(any_value::Value::StringValue(values.value(i).to_string())),
            })
        };
        kvs.push(KeyValue { key, value: val });
    }
    any_value::Value::KvlistValue(KeyValueList { values: kvs })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client;
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
    use crate::store::Store;

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
        let (mut store, _rx) = Store::new(100, usize::MAX, usize::MAX, usize::MAX);

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

        store.insert_logs(vec![
            ResourceLogs {
                resource: make_resource("frontend"),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1000,
                        observed_time_unix_nano: 0,
                        severity_number: 9,
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
                        severity_number: 17,
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

    fn setup_ctx(store: &Store) -> SessionContext {
        let ctx = SessionContext::new();
        let traces_batch = crate::query::arrow_convert::traces_to_batch(store);
        let logs_batch = crate::query::arrow_convert::logs_to_batch(store);
        let metrics_batch = crate::query::arrow_convert::metrics_to_batch(store);

        ctx.register_batch("traces", traces_batch).unwrap();
        ctx.register_batch("logs", logs_batch).unwrap();
        ctx.register_batch("metrics", metrics_batch).unwrap();
        ctx
    }

    fn get_str(row: &ProtoRow, name: &str) -> String {
        client::get_row_string(row, name).unwrap_or_default()
    }

    fn get_int(row: &ProtoRow, name: &str) -> Option<i64> {
        row.columns.iter().find(|c| c.name == name).and_then(|c| {
            c.value.as_ref().and_then(|v| match &v.value {
                Some(any_value::Value::IntValue(i)) => Some(*i),
                _ => None,
            })
        })
    }

    fn get_double(row: &ProtoRow, name: &str) -> Option<f64> {
        row.columns.iter().find(|c| c.name == name).and_then(|c| {
            c.value.as_ref().and_then(|v| match &v.value {
                Some(any_value::Value::DoubleValue(d)) => Some(*d),
                _ => None,
            })
        })
    }

    #[tokio::test]
    async fn end_to_end_traces_query() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(&ctx, "SELECT * FROM traces WHERE service_name = 'frontend'")
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        let row = &result[0];
        assert!(row.columns.iter().any(|c| c.name == "trace_id"));
        assert!(row.columns.iter().any(|c| c.name == "span_name"));
        assert!(row.columns.iter().any(|c| c.name == "resource"));
        assert!(row.columns.iter().any(|c| c.name == "attributes"));
    }

    #[tokio::test]
    async fn end_to_end_logs_query() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(&ctx, "SELECT * FROM logs WHERE severity_number >= 17")
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn end_to_end_metrics_query() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(
            &ctx,
            "SELECT * FROM metrics WHERE metric_name = 'http.duration'",
        )
        .await
        .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn end_to_end_with_limit() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(&ctx, "SELECT * FROM traces LIMIT 1").await.unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn end_to_end_trace_aware_limit() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        // Limit to 1 trace — should return all spans of that trace (1 span per trace in test data)
        let sql = convert::trace_flags_to_sql(None, None, &[], Some(1), None, None);
        let result = execute(&ctx, &sql).await.unwrap();
        assert_eq!(result.len(), 1);

        // Limit to 2 traces — should return both traces
        let sql = convert::trace_flags_to_sql(None, None, &[], Some(2), None, None);
        let result = execute(&ctx, &sql).await.unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn end_to_end_trace_aware_limit_with_filter() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let sql = convert::trace_flags_to_sql(Some("frontend"), None, &[], Some(10), None, None);
        let result = execute(&ctx, &sql).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(get_str(&result[0], "service_name"), "frontend");
    }

    #[tokio::test]
    async fn end_to_end_attribute_filter() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(
            &ctx,
            "SELECT * FROM traces WHERE attributes['http.method'] = 'GET'",
        )
        .await
        .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn end_to_end_all_traces() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(&ctx, "SELECT * FROM traces").await.unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn end_to_end_all_logs() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(&ctx, "SELECT * FROM logs").await.unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn end_to_end_all_metrics() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(&ctx, "SELECT * FROM metrics").await.unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn projection_specific_columns_traces() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(&ctx, "SELECT span_name, service_name FROM traces")
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        for row in &result {
            assert_eq!(row.columns.len(), 2);
            assert_eq!(row.columns[0].name, "span_name");
            assert_eq!(row.columns[1].name, "service_name");
        }
    }

    #[tokio::test]
    async fn projection_specific_columns_logs() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(&ctx, "SELECT timestamp, severity FROM logs")
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        for row in &result {
            assert_eq!(row.columns.len(), 2);
            assert_eq!(row.columns[0].name, "timestamp");
            assert_eq!(row.columns[1].name, "severity");
        }
    }

    #[tokio::test]
    async fn projection_resource_column() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(
            &ctx,
            "SELECT span_name, resource FROM traces WHERE service_name = 'frontend'",
        )
        .await
        .unwrap();
        assert_eq!(result.len(), 1);
        let row = &result[0];
        assert_eq!(row.columns[0].name, "span_name");
        assert_eq!(row.columns[1].name, "resource");
        match &row.columns[1].value {
            Some(v) => match &v.value {
                Some(any_value::Value::KvlistValue(kvl)) => {
                    assert!(kvl.values.iter().any(|kv| kv.key == "service.name"));
                }
                _ => panic!("expected KvlistValue for resource"),
            },
            None => panic!("expected non-null resource"),
        }
    }

    #[tokio::test]
    async fn aggregation_count() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(
            &ctx,
            "SELECT service_name, COUNT(*) as cnt FROM traces GROUP BY service_name ORDER BY service_name",
        )
        .await
        .unwrap();
        assert_eq!(result.len(), 2);
        // backend, frontend (alphabetical order)
        assert_eq!(get_str(&result[0], "service_name"), "backend");
        assert_eq!(get_int(&result[0], "cnt"), Some(1));
        assert_eq!(get_str(&result[1], "service_name"), "frontend");
        assert_eq!(get_int(&result[1], "cnt"), Some(1));
    }

    #[tokio::test]
    async fn aggregation_avg() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(&ctx, "SELECT AVG(duration_ns) as avg_dur FROM traces")
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        // (1000 + 3000) / 2 = 2000
        let avg = get_double(&result[0], "avg_dur").expect("expected double");
        assert!((avg - 2000.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn severity_count_group_by() {
        let store = setup_store();
        let ctx = setup_ctx(&store);
        let result = execute(
            &ctx,
            "SELECT severity, COUNT(*) as cnt FROM logs GROUP BY severity ORDER BY severity",
        )
        .await
        .unwrap();
        assert_eq!(result.len(), 2);
    }
}
