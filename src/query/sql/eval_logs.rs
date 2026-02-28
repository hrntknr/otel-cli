use crate::client::{extract_any_value_string, get_service_name};
use crate::proto::opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs};
use crate::store::{severity_text_to_number, Store};

use super::eval_traces::{
    compare_field_value, compare_sort_values, field_value_to_string, like_match, lookup_attribute,
    regex_match, FieldValue, SortValue,
};
use super::parser::{ColumnRef, CompOp, SqlQuery, SqlValue, WhereExpr};

pub fn eval_logs(store: &Store, query: &SqlQuery) -> Vec<ResourceLogs> {
    let mut results: Vec<ResourceLogs> = store
        .all_logs()
        .iter()
        .filter(|rl| match &query.where_expr {
            Some(expr) => resource_logs_matches(rl, expr),
            None => true,
        })
        .cloned()
        .collect();

    // ORDER BY
    if !query.order_by.is_empty() {
        let ob = &query.order_by[0];
        let col = ob.column.as_str();
        let desc = ob.desc;
        results.sort_by(|a, b| {
            let va = resource_logs_sort_value(a, col);
            let vb = resource_logs_sort_value(b, col);
            let cmp = compare_sort_values(&va, &vb);
            if desc {
                cmp.reverse()
            } else {
                cmp
            }
        });
    }

    // LIMIT
    if let Some(limit) = query.limit {
        results.truncate(limit);
    }

    results
}

fn resource_logs_sort_value(rl: &ResourceLogs, column: &str) -> SortValue {
    for sl in &rl.scope_logs {
        for lr in &sl.log_records {
            return log_record_sort_value(lr, &rl.resource, column);
        }
    }
    SortValue::Null
}

fn log_record_sort_value(
    lr: &LogRecord,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    column: &str,
) -> SortValue {
    let fv = resolve_log_column(lr, resource, &ColumnRef::Named(column.to_string()));
    match fv {
        FieldValue::String(s) => SortValue::String(s),
        FieldValue::Number(n) => SortValue::Number(n),
        FieldValue::Null => SortValue::Null,
    }
}

fn resource_logs_matches(rl: &ResourceLogs, expr: &WhereExpr) -> bool {
    // A ResourceLogs matches if ANY log record matches
    rl.scope_logs.iter().any(|sl| {
        sl.log_records
            .iter()
            .any(|lr| eval_where_expr_for_log(lr, &rl.resource, expr))
    })
}

fn eval_where_expr_for_log(
    lr: &LogRecord,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    expr: &WhereExpr,
) -> bool {
    match expr {
        WhereExpr::Comparison { column, op, value } => {
            // Special handling for severity comparisons using severity_text_to_number
            if matches!(column, ColumnRef::Named(n) if n == "severity") {
                if let SqlValue::String(sev_text) = value {
                    if let (Some(record_num), Some(threshold_num)) = (
                        severity_text_to_number(&lr.severity_text),
                        severity_text_to_number(sev_text),
                    ) {
                        return compare_field_value(
                            &FieldValue::Number(record_num as f64),
                            op,
                            &SqlValue::Number(threshold_num as f64),
                        );
                    }
                }
            }
            let field_val = resolve_log_column(lr, resource, column);
            compare_field_value(&field_val, op, value)
        }
        WhereExpr::Like {
            column,
            pattern,
            negated,
        } => {
            let field_val = resolve_log_column(lr, resource, column);
            let s = field_value_to_string(&field_val);
            let matched = like_match(&s, pattern);
            if *negated {
                !matched
            } else {
                matched
            }
        }
        WhereExpr::RegexMatch {
            column,
            pattern,
            negated,
        } => {
            let field_val = resolve_log_column(lr, resource, column);
            let s = field_value_to_string(&field_val);
            let matched = regex_match(&s, pattern);
            if *negated {
                !matched
            } else {
                matched
            }
        }
        WhereExpr::InList {
            column,
            values,
            negated,
        } => {
            let field_val = resolve_log_column(lr, resource, column);
            let matched = values
                .iter()
                .any(|v| compare_field_value(&field_val, &CompOp::Eq, v));
            if *negated {
                !matched
            } else {
                matched
            }
        }
        WhereExpr::IsNull { column, negated } => {
            let field_val = resolve_log_column(lr, resource, column);
            let is_null = matches!(field_val, FieldValue::Null);
            if *negated {
                !is_null
            } else {
                is_null
            }
        }
        WhereExpr::And(left, right) => {
            eval_where_expr_for_log(lr, resource, left)
                && eval_where_expr_for_log(lr, resource, right)
        }
        WhereExpr::Or(left, right) => {
            eval_where_expr_for_log(lr, resource, left)
                || eval_where_expr_for_log(lr, resource, right)
        }
        WhereExpr::Not(inner) => !eval_where_expr_for_log(lr, resource, inner),
    }
}

pub(crate) fn resolve_log_column_pub(
    lr: &LogRecord,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    column: &ColumnRef,
) -> FieldValue {
    resolve_log_column(lr, resource, column)
}

fn resolve_log_column(
    lr: &LogRecord,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    column: &ColumnRef,
) -> FieldValue {
    match column {
        ColumnRef::Named(name) => match name.as_str() {
            "timestamp" => FieldValue::Number(lr.time_unix_nano as f64),
            "severity" => FieldValue::String(lr.severity_text.clone()),
            "severity_number" => FieldValue::Number(lr.severity_number as f64),
            "body" => match &lr.body {
                Some(v) => FieldValue::String(extract_any_value_string(v)),
                None => FieldValue::Null,
            },
            "service_name" => FieldValue::String(get_service_name(resource)),
            _ => FieldValue::Null,
        },
        ColumnRef::BracketAccess(base, key) => match base.as_str() {
            "attributes" => lookup_attribute(&lr.attributes, key),
            "resource" => {
                let attrs = resource
                    .as_ref()
                    .map(|r| r.attributes.as_slice())
                    .unwrap_or_default();
                lookup_attribute(attrs, key)
            }
            _ => FieldValue::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::opentelemetry::proto::{
        common::v1::{any_value, AnyValue, KeyValue},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };
    use crate::query::sql::parser::Projection;
    use crate::query::TargetTable;
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

    fn make_log(
        service_name: &str,
        severity: &str,
        body: Option<&str>,
        attrs: Vec<KeyValue>,
        time_ns: u64,
    ) -> ResourceLogs {
        let severity_number = severity_text_to_number(severity).unwrap_or(0);
        ResourceLogs {
            resource: make_resource(service_name),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: time_ns,
                    observed_time_unix_nano: 0,
                    severity_number,
                    severity_text: severity.to_string(),
                    body: body.map(|b| AnyValue {
                        value: Some(any_value::Value::StringValue(b.to_string())),
                    }),
                    attributes: attrs,
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    event_name: String::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }
    }

    fn make_query(where_expr: Option<WhereExpr>) -> SqlQuery {
        SqlQuery {
            table: TargetTable::Logs,
            where_expr,
            limit: None,
            order_by: vec![],
            projection: Projection::All,
        }
    }

    fn setup_store() -> Store {
        let (mut store, _rx) = Store::new(100);
        store.insert_logs(vec![
            make_log("frontend", "INFO", Some("request started"), vec![], 1000),
            make_log(
                "backend",
                "ERROR",
                Some("db connection failed"),
                vec![make_kv("db", "postgres")],
                2000,
            ),
            make_log("frontend", "WARN", Some("slow response"), vec![], 3000),
            make_log(
                "backend",
                "DEBUG",
                Some("query executed"),
                vec![make_kv("db", "redis")],
                4000,
            ),
        ]);
        store
    }

    #[test]
    fn eval_no_filter() {
        let store = setup_store();
        let query = make_query(None);
        let result = eval_logs(&store, &query);
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn eval_filter_by_service_name() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::Named("service_name".to_string()),
            op: CompOp::Eq,
            value: SqlValue::String("frontend".to_string()),
        }));
        let result = eval_logs(&store, &query);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn eval_filter_by_severity_ge() {
        let store = setup_store();
        // severity >= 'WARN' should match WARN(13) and ERROR(17)
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::Named("severity".to_string()),
            op: CompOp::GtEq,
            value: SqlValue::String("WARN".to_string()),
        }));
        let result = eval_logs(&store, &query);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn eval_filter_by_severity_eq() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::Named("severity".to_string()),
            op: CompOp::Eq,
            value: SqlValue::String("ERROR".to_string()),
        }));
        let result = eval_logs(&store, &query);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn eval_filter_body_like() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Like {
            column: ColumnRef::Named("body".to_string()),
            pattern: "%connection%".to_string(),
            negated: false,
        }));
        let result = eval_logs(&store, &query);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn eval_filter_by_attribute() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::BracketAccess("attributes".to_string(), "db".to_string()),
            op: CompOp::Eq,
            value: SqlValue::String("postgres".to_string()),
        }));
        let result = eval_logs(&store, &query);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn eval_with_limit() {
        let store = setup_store();
        let mut query = make_query(None);
        query.limit = Some(2);
        let result = eval_logs(&store, &query);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn eval_filter_and_severity_service() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::And(
            Box::new(WhereExpr::Comparison {
                column: ColumnRef::Named("service_name".to_string()),
                op: CompOp::Eq,
                value: SqlValue::String("backend".to_string()),
            }),
            Box::new(WhereExpr::Comparison {
                column: ColumnRef::Named("severity".to_string()),
                op: CompOp::GtEq,
                value: SqlValue::String("ERROR".to_string()),
            }),
        )));
        let result = eval_logs(&store, &query);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn eval_filter_body_is_not_null() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::IsNull {
            column: ColumnRef::Named("body".to_string()),
            negated: true,
        }));
        let result = eval_logs(&store, &query);
        assert_eq!(result.len(), 4);
    }
}
