use crate::client::{extract_any_value_string, get_service_name, hex_encode};
use crate::proto::opentelemetry::proto::common::v1::KeyValue;
use crate::proto::opentelemetry::proto::trace::v1::Span;
use crate::store::{Store, TraceGroup};

use super::parser::{ColumnRef, CompOp, SqlQuery, SqlValue, WhereExpr};

pub fn eval_traces(store: &Store, query: &SqlQuery) -> Vec<TraceGroup> {
    let mut results: Vec<TraceGroup> = store
        .all_traces()
        .iter()
        .filter(|group| match &query.where_expr {
            Some(expr) => trace_group_matches(group, expr),
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
            let va = trace_group_sort_value(a, col);
            let vb = trace_group_sort_value(b, col);
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

fn trace_group_sort_value(group: &TraceGroup, column: &str) -> SortValue {
    // Use the first span's value for sorting
    for rs in &group.resource_spans {
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                return span_column_sort_value(span, &rs.resource, column);
            }
        }
    }
    SortValue::Null
}

fn trace_group_matches(group: &TraceGroup, expr: &WhereExpr) -> bool {
    // A TraceGroup matches if ANY span matches
    group.resource_spans.iter().any(|rs| {
        rs.scope_spans.iter().any(|ss| {
            ss.spans
                .iter()
                .any(|span| eval_where_expr_for_span(span, &rs.resource, expr))
        })
    })
}

fn eval_where_expr_for_span(
    span: &Span,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    expr: &WhereExpr,
) -> bool {
    match expr {
        WhereExpr::Comparison { column, op, value } => {
            let field_val = resolve_span_column(span, resource, column);
            compare_field_value(&field_val, op, value)
        }
        WhereExpr::Like {
            column,
            pattern,
            negated,
        } => {
            let field_val = resolve_span_column(span, resource, column);
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
            let field_val = resolve_span_column(span, resource, column);
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
            let field_val = resolve_span_column(span, resource, column);
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
            let field_val = resolve_span_column(span, resource, column);
            let is_null = matches!(field_val, FieldValue::Null);
            if *negated {
                !is_null
            } else {
                is_null
            }
        }
        WhereExpr::And(left, right) => {
            eval_where_expr_for_span(span, resource, left)
                && eval_where_expr_for_span(span, resource, right)
        }
        WhereExpr::Or(left, right) => {
            eval_where_expr_for_span(span, resource, left)
                || eval_where_expr_for_span(span, resource, right)
        }
        WhereExpr::Not(inner) => !eval_where_expr_for_span(span, resource, inner),
    }
}

fn resolve_span_column(
    span: &Span,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    column: &ColumnRef,
) -> FieldValue {
    match column {
        ColumnRef::Named(name) => match name.as_str() {
            "trace_id" => FieldValue::String(hex_encode(&span.trace_id)),
            "span_id" => FieldValue::String(hex_encode(&span.span_id)),
            "parent_span_id" => {
                if span.parent_span_id.is_empty() {
                    FieldValue::Null
                } else {
                    FieldValue::String(hex_encode(&span.parent_span_id))
                }
            }
            "service_name" => FieldValue::String(get_service_name(resource)),
            "span_name" => FieldValue::String(span.name.clone()),
            "kind" => FieldValue::Number(span.kind as f64),
            "status_code" => {
                let code = span.status.as_ref().map(|s| s.code).unwrap_or(0);
                FieldValue::Number(code as f64)
            }
            "start_time" => FieldValue::Number(span.start_time_unix_nano as f64),
            "end_time" => FieldValue::Number(span.end_time_unix_nano as f64),
            "duration_ns" => FieldValue::Number(
                (span.end_time_unix_nano as f64) - (span.start_time_unix_nano as f64),
            ),
            _ => FieldValue::Null,
        },
        ColumnRef::BracketAccess(base, key) => match base.as_str() {
            "attributes" => lookup_attribute(&span.attributes, key),
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

fn span_column_sort_value(
    span: &Span,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    column: &str,
) -> SortValue {
    let fv = resolve_span_column(span, resource, &ColumnRef::Named(column.to_string()));
    match fv {
        FieldValue::String(s) => SortValue::String(s),
        FieldValue::Number(n) => SortValue::Number(n),
        FieldValue::Null => SortValue::Null,
    }
}

// --- Shared field value types and comparison helpers ---

#[derive(Debug, Clone)]
pub(crate) enum FieldValue {
    String(String),
    Number(f64),
    Null,
}

#[derive(Debug, Clone)]
pub(crate) enum SortValue {
    String(String),
    Number(f64),
    Null,
}

pub(crate) fn compare_sort_values(a: &SortValue, b: &SortValue) -> std::cmp::Ordering {
    match (a, b) {
        (SortValue::Number(a), SortValue::Number(b)) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (SortValue::String(a), SortValue::String(b)) => a.cmp(b),
        (SortValue::Null, SortValue::Null) => std::cmp::Ordering::Equal,
        (SortValue::Null, _) => std::cmp::Ordering::Less,
        (_, SortValue::Null) => std::cmp::Ordering::Greater,
        (SortValue::Number(_), SortValue::String(_)) => std::cmp::Ordering::Less,
        (SortValue::String(_), SortValue::Number(_)) => std::cmp::Ordering::Greater,
    }
}

pub(crate) fn compare_field_value(field: &FieldValue, op: &CompOp, value: &SqlValue) -> bool {
    match (field, value) {
        (FieldValue::String(s), SqlValue::String(v)) => match op {
            CompOp::Eq => s == v,
            CompOp::NotEq => s != v,
            CompOp::Lt => s < v,
            CompOp::Gt => s > v,
            CompOp::LtEq => s <= v,
            CompOp::GtEq => s >= v,
        },
        (FieldValue::Number(n), SqlValue::Number(v)) => match op {
            CompOp::Eq => *n == *v,
            CompOp::NotEq => *n != *v,
            CompOp::Lt => *n < *v,
            CompOp::Gt => *n > *v,
            CompOp::LtEq => *n <= *v,
            CompOp::GtEq => *n >= *v,
        },
        // Cross-type: try numeric comparison for string field vs number value
        (FieldValue::String(s), SqlValue::Number(v)) => {
            if let Ok(n) = s.parse::<f64>() {
                match op {
                    CompOp::Eq => n == *v,
                    CompOp::NotEq => n != *v,
                    CompOp::Lt => n < *v,
                    CompOp::Gt => n > *v,
                    CompOp::LtEq => n <= *v,
                    CompOp::GtEq => n >= *v,
                }
            } else {
                matches!(op, CompOp::NotEq)
            }
        }
        (FieldValue::Number(n), SqlValue::String(v)) => {
            if let Ok(pv) = v.parse::<f64>() {
                match op {
                    CompOp::Eq => *n == pv,
                    CompOp::NotEq => *n != pv,
                    CompOp::Lt => *n < pv,
                    CompOp::Gt => *n > pv,
                    CompOp::LtEq => *n <= pv,
                    CompOp::GtEq => *n >= pv,
                }
            } else {
                matches!(op, CompOp::NotEq)
            }
        }
        (FieldValue::Null, _) => matches!(op, CompOp::NotEq),
        (_, SqlValue::Boolean(_)) => {
            // Boolean comparisons: convert field to bool if possible
            matches!(op, CompOp::NotEq)
        }
    }
}

pub(crate) fn field_value_to_string(fv: &FieldValue) -> String {
    match fv {
        FieldValue::String(s) => s.clone(),
        FieldValue::Number(n) => n.to_string(),
        FieldValue::Null => String::new(),
    }
}

pub(crate) fn like_match(value: &str, pattern: &str) -> bool {
    // SQL LIKE: % matches any sequence, _ matches any single char
    let regex_pattern = format!("^{}$", pattern.replace('%', ".*").replace('_', "."));
    regex::Regex::new(&regex_pattern)
        .map(|re| re.is_match(value))
        .unwrap_or(false)
}

pub(crate) fn regex_match(value: &str, pattern: &str) -> bool {
    regex::Regex::new(pattern)
        .map(|re| re.is_match(value))
        .unwrap_or(false)
}

pub(crate) fn lookup_attribute(attrs: &[KeyValue], key: &str) -> FieldValue {
    for kv in attrs {
        if kv.key == key {
            return match &kv.value {
                Some(v) => {
                    use crate::proto::opentelemetry::proto::common::v1::any_value;
                    match &v.value {
                        Some(any_value::Value::StringValue(s)) => FieldValue::String(s.clone()),
                        Some(any_value::Value::IntValue(i)) => FieldValue::Number(*i as f64),
                        Some(any_value::Value::DoubleValue(d)) => FieldValue::Number(*d),
                        Some(any_value::Value::BoolValue(b)) => FieldValue::String(b.to_string()),
                        _ => FieldValue::String(extract_any_value_string(v)),
                    }
                }
                None => FieldValue::Null,
            };
        }
    }
    FieldValue::Null
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::opentelemetry::proto::{
        common::v1::{any_value, AnyValue, KeyValue},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
    };
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

    fn make_kv_int(key: &str, value: i64) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(value)),
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

    fn make_span(
        trace_id: &[u8],
        span_id: &[u8],
        name: &str,
        start: u64,
        end: u64,
        attrs: Vec<KeyValue>,
    ) -> Span {
        Span {
            trace_id: trace_id.to_vec(),
            span_id: span_id.to_vec(),
            trace_state: String::new(),
            parent_span_id: vec![],
            flags: 0,
            name: name.to_string(),
            kind: 2, // SERVER
            start_time_unix_nano: start,
            end_time_unix_nano: end,
            attributes: attrs,
            dropped_attributes_count: 0,
            events: vec![],
            dropped_events_count: 0,
            links: vec![],
            dropped_links_count: 0,
            status: Some(Status {
                message: String::new(),
                code: 0,
            }),
        }
    }

    fn make_rs(service_name: &str, spans: Vec<Span>) -> ResourceSpans {
        ResourceSpans {
            resource: make_resource(service_name),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }
    }

    fn make_query(where_expr: Option<WhereExpr>) -> SqlQuery {
        SqlQuery {
            table: TargetTable::Traces,
            where_expr,
            limit: None,
            order_by: vec![],
            select_all: true,
        }
    }

    fn setup_store() -> Store {
        let (mut store, _rx) = Store::new(100);
        store.insert_traces(vec![
            make_rs(
                "frontend",
                vec![make_span(
                    &[1; 16],
                    &[0, 0, 0, 0, 0, 0, 0, 1],
                    "GET /api/users",
                    1000,
                    2000,
                    vec![
                        make_kv("http.method", "GET"),
                        make_kv_int("http.status_code", 200),
                    ],
                )],
            ),
            make_rs(
                "backend",
                vec![make_span(
                    &[2; 16],
                    &[0, 0, 0, 0, 0, 0, 0, 2],
                    "POST /api/orders",
                    2000,
                    5000,
                    vec![
                        make_kv("http.method", "POST"),
                        make_kv_int("http.status_code", 500),
                    ],
                )],
            ),
            make_rs(
                "frontend",
                vec![make_span(
                    &[3; 16],
                    &[0, 0, 0, 0, 0, 0, 0, 3],
                    "GET /health",
                    3000,
                    3100,
                    vec![make_kv("http.method", "GET")],
                )],
            ),
        ]);
        store
    }

    #[test]
    fn eval_no_filter() {
        let store = setup_store();
        let query = make_query(None);
        let result = eval_traces(&store, &query);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn eval_filter_by_service_name() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::Named("service_name".to_string()),
            op: CompOp::Eq,
            value: SqlValue::String("frontend".to_string()),
        }));
        let result = eval_traces(&store, &query);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn eval_filter_by_span_name_like() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Like {
            column: ColumnRef::Named("span_name".to_string()),
            pattern: "%api%".to_string(),
            negated: false,
        }));
        let result = eval_traces(&store, &query);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn eval_filter_by_attribute() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::BracketAccess("attributes".to_string(), "http.method".to_string()),
            op: CompOp::Eq,
            value: SqlValue::String("POST".to_string()),
        }));
        let result = eval_traces(&store, &query);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn eval_filter_by_duration() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::Named("duration_ns".to_string()),
            op: CompOp::Gt,
            value: SqlValue::Number(1000.0),
        }));
        let result = eval_traces(&store, &query);
        // frontend span 1: duration = 2000 - 1000 = 1000, not > 1000
        // backend span: duration = 5000 - 2000 = 3000, > 1000 ✓
        // frontend span 3: duration = 3100 - 3000 = 100, not > 1000
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn eval_filter_and() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::And(
            Box::new(WhereExpr::Comparison {
                column: ColumnRef::Named("service_name".to_string()),
                op: CompOp::Eq,
                value: SqlValue::String("frontend".to_string()),
            }),
            Box::new(WhereExpr::Like {
                column: ColumnRef::Named("span_name".to_string()),
                pattern: "%api%".to_string(),
                negated: false,
            }),
        )));
        let result = eval_traces(&store, &query);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn eval_with_limit() {
        let store = setup_store();
        let mut query = make_query(None);
        query.limit = Some(2);
        let result = eval_traces(&store, &query);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn eval_filter_in_list() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::InList {
            column: ColumnRef::Named("service_name".to_string()),
            values: vec![
                SqlValue::String("frontend".to_string()),
                SqlValue::String("backend".to_string()),
            ],
            negated: false,
        }));
        let result = eval_traces(&store, &query);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn eval_filter_regex() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::RegexMatch {
            column: ColumnRef::Named("span_name".to_string()),
            pattern: "^GET.*".to_string(),
            negated: false,
        }));
        let result = eval_traces(&store, &query);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn eval_filter_is_null() {
        let store = setup_store();
        // parent_span_id is empty for all spans, which maps to Null
        let query = make_query(Some(WhereExpr::IsNull {
            column: ColumnRef::Named("parent_span_id".to_string()),
            negated: false,
        }));
        let result = eval_traces(&store, &query);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn eval_filter_numeric_attribute() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::BracketAccess(
                "attributes".to_string(),
                "http.status_code".to_string(),
            ),
            op: CompOp::GtEq,
            value: SqlValue::Number(500.0),
        }));
        let result = eval_traces(&store, &query);
        assert_eq!(result.len(), 1);
    }
}
