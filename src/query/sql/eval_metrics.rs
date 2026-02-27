use crate::client::get_service_name;
use crate::proto::opentelemetry::proto::metrics::v1::{
    metric, number_data_point, Metric, ResourceMetrics,
};
use crate::store::Store;

use super::eval_traces::{
    compare_field_value, compare_sort_values, field_value_to_string, like_match, lookup_attribute,
    regex_match, FieldValue, SortValue,
};
use super::parser::{ColumnRef, CompOp, SqlQuery, WhereExpr};

pub fn eval_metrics(store: &Store, query: &SqlQuery) -> Vec<ResourceMetrics> {
    let mut results: Vec<ResourceMetrics> = store
        .all_metrics()
        .iter()
        .filter(|rm| match &query.where_expr {
            Some(expr) => resource_metrics_matches(rm, expr),
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
            let va = resource_metrics_sort_value(a, col);
            let vb = resource_metrics_sort_value(b, col);
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

fn resource_metrics_sort_value(rm: &ResourceMetrics, column: &str) -> SortValue {
    for sm in &rm.scope_metrics {
        for m in &sm.metrics {
            if let Some(sv) = first_data_point_sort_value(m, &rm.resource, column) {
                return sv;
            }
        }
    }
    SortValue::Null
}

fn first_data_point_sort_value(
    metric: &Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    column: &str,
) -> Option<SortValue> {
    let col_ref = ColumnRef::Named(column.to_string());
    let metric_type = metric_type_name(metric);
    match &metric.data {
        Some(metric::Data::Gauge(g)) => {
            for dp in &g.data_points {
                let fv = resolve_metric_column(dp, metric, resource, &col_ref, &metric_type);
                return Some(field_to_sort(fv));
            }
            None
        }
        Some(metric::Data::Sum(s)) => {
            for dp in &s.data_points {
                let fv = resolve_metric_column(dp, metric, resource, &col_ref, &metric_type);
                return Some(field_to_sort(fv));
            }
            None
        }
        _ => None,
    }
}

fn field_to_sort(fv: FieldValue) -> SortValue {
    match fv {
        FieldValue::String(s) => SortValue::String(s),
        FieldValue::Number(n) => SortValue::Number(n),
        FieldValue::Null => SortValue::Null,
    }
}

fn resource_metrics_matches(rm: &ResourceMetrics, expr: &WhereExpr) -> bool {
    let metric_type_cache: Vec<(&Metric, String)> = rm
        .scope_metrics
        .iter()
        .flat_map(|sm| sm.metrics.iter().map(|m| (m, metric_type_name(m))))
        .collect();

    for (metric, metric_type) in &metric_type_cache {
        if metric_data_matches(metric, &rm.resource, expr, metric_type) {
            return true;
        }
    }
    false
}

fn metric_data_matches(
    metric: &Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    expr: &WhereExpr,
    metric_type: &str,
) -> bool {
    match &metric.data {
        Some(metric::Data::Gauge(g)) => g
            .data_points
            .iter()
            .any(|dp| eval_where_expr_for_data_point(dp, metric, resource, expr, metric_type)),
        Some(metric::Data::Sum(s)) => s
            .data_points
            .iter()
            .any(|dp| eval_where_expr_for_data_point(dp, metric, resource, expr, metric_type)),
        Some(metric::Data::Histogram(h)) => h
            .data_points
            .iter()
            .any(|dp| eval_where_expr_for_histogram(dp, metric, resource, expr, metric_type)),
        Some(metric::Data::ExponentialHistogram(eh)) => eh
            .data_points
            .iter()
            .any(|dp| eval_where_expr_for_exp_histogram(dp, metric, resource, expr, metric_type)),
        Some(metric::Data::Summary(s)) => s
            .data_points
            .iter()
            .any(|dp| eval_where_expr_for_summary(dp, metric, resource, expr, metric_type)),
        None => false,
    }
}

fn eval_where_expr_for_data_point(
    dp: &crate::proto::opentelemetry::proto::metrics::v1::NumberDataPoint,
    metric: &Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    expr: &WhereExpr,
    metric_type: &str,
) -> bool {
    eval_where_generic(
        |col| resolve_metric_column(dp, metric, resource, col, metric_type),
        expr,
    )
}

fn eval_where_expr_for_histogram(
    dp: &crate::proto::opentelemetry::proto::metrics::v1::HistogramDataPoint,
    metric: &Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    expr: &WhereExpr,
    metric_type: &str,
) -> bool {
    eval_where_generic(
        |col| resolve_histogram_column(dp, metric, resource, col, metric_type),
        expr,
    )
}

fn eval_where_expr_for_exp_histogram(
    dp: &crate::proto::opentelemetry::proto::metrics::v1::ExponentialHistogramDataPoint,
    metric: &Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    expr: &WhereExpr,
    metric_type: &str,
) -> bool {
    eval_where_generic(
        |col| resolve_exp_histogram_column(dp, metric, resource, col, metric_type),
        expr,
    )
}

fn eval_where_expr_for_summary(
    dp: &crate::proto::opentelemetry::proto::metrics::v1::SummaryDataPoint,
    metric: &Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    expr: &WhereExpr,
    metric_type: &str,
) -> bool {
    eval_where_generic(
        |col| resolve_summary_column(dp, metric, resource, col, metric_type),
        expr,
    )
}

fn eval_where_generic(resolve: impl Fn(&ColumnRef) -> FieldValue + Copy, expr: &WhereExpr) -> bool {
    match expr {
        WhereExpr::Comparison { column, op, value } => {
            let field_val = resolve(column);
            compare_field_value(&field_val, op, value)
        }
        WhereExpr::Like {
            column,
            pattern,
            negated,
        } => {
            let field_val = resolve(column);
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
            let field_val = resolve(column);
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
            let field_val = resolve(column);
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
            let field_val = resolve(column);
            let is_null = matches!(field_val, FieldValue::Null);
            if *negated {
                !is_null
            } else {
                is_null
            }
        }
        WhereExpr::And(left, right) => {
            eval_where_generic(resolve, left) && eval_where_generic(resolve, right)
        }
        WhereExpr::Or(left, right) => {
            eval_where_generic(resolve, left) || eval_where_generic(resolve, right)
        }
        WhereExpr::Not(inner) => !eval_where_generic(resolve, inner),
    }
}

fn resolve_metric_column(
    dp: &crate::proto::opentelemetry::proto::metrics::v1::NumberDataPoint,
    metric: &Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    column: &ColumnRef,
    metric_type: &str,
) -> FieldValue {
    match column {
        ColumnRef::Named(name) => match name.as_str() {
            "timestamp" => FieldValue::Number(dp.time_unix_nano as f64),
            "metric_name" => FieldValue::String(metric.name.clone()),
            "type" => FieldValue::String(metric_type.to_string()),
            "value" => match &dp.value {
                Some(number_data_point::Value::AsDouble(d)) => FieldValue::Number(*d),
                Some(number_data_point::Value::AsInt(i)) => FieldValue::Number(*i as f64),
                None => FieldValue::Null,
            },
            "count" => FieldValue::Null,
            "sum" => FieldValue::Null,
            "service_name" => FieldValue::String(get_service_name(resource)),
            _ => FieldValue::Null,
        },
        ColumnRef::BracketAccess(base, key) => match base.as_str() {
            "attributes" => lookup_attribute(&dp.attributes, key),
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

fn resolve_histogram_column(
    dp: &crate::proto::opentelemetry::proto::metrics::v1::HistogramDataPoint,
    metric: &Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    column: &ColumnRef,
    metric_type: &str,
) -> FieldValue {
    match column {
        ColumnRef::Named(name) => match name.as_str() {
            "timestamp" => FieldValue::Number(dp.time_unix_nano as f64),
            "metric_name" => FieldValue::String(metric.name.clone()),
            "type" => FieldValue::String(metric_type.to_string()),
            "value" => FieldValue::Null,
            "count" => FieldValue::Number(dp.count as f64),
            "sum" => match dp.sum {
                Some(s) => FieldValue::Number(s),
                None => FieldValue::Null,
            },
            "service_name" => FieldValue::String(get_service_name(resource)),
            _ => FieldValue::Null,
        },
        ColumnRef::BracketAccess(base, key) => match base.as_str() {
            "attributes" => lookup_attribute(&dp.attributes, key),
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

fn resolve_exp_histogram_column(
    dp: &crate::proto::opentelemetry::proto::metrics::v1::ExponentialHistogramDataPoint,
    metric: &Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    column: &ColumnRef,
    metric_type: &str,
) -> FieldValue {
    match column {
        ColumnRef::Named(name) => match name.as_str() {
            "timestamp" => FieldValue::Number(dp.time_unix_nano as f64),
            "metric_name" => FieldValue::String(metric.name.clone()),
            "type" => FieldValue::String(metric_type.to_string()),
            "value" => FieldValue::Null,
            "count" => FieldValue::Number(dp.count as f64),
            "sum" => match dp.sum {
                Some(s) => FieldValue::Number(s),
                None => FieldValue::Null,
            },
            "service_name" => FieldValue::String(get_service_name(resource)),
            _ => FieldValue::Null,
        },
        ColumnRef::BracketAccess(base, key) => match base.as_str() {
            "attributes" => lookup_attribute(&dp.attributes, key),
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

fn resolve_summary_column(
    dp: &crate::proto::opentelemetry::proto::metrics::v1::SummaryDataPoint,
    metric: &Metric,
    resource: &Option<crate::proto::opentelemetry::proto::resource::v1::Resource>,
    column: &ColumnRef,
    metric_type: &str,
) -> FieldValue {
    match column {
        ColumnRef::Named(name) => match name.as_str() {
            "timestamp" => FieldValue::Number(dp.time_unix_nano as f64),
            "metric_name" => FieldValue::String(metric.name.clone()),
            "type" => FieldValue::String(metric_type.to_string()),
            "value" => FieldValue::Null,
            "count" => FieldValue::Number(dp.count as f64),
            "sum" => FieldValue::Number(dp.sum),
            "service_name" => FieldValue::String(get_service_name(resource)),
            _ => FieldValue::Null,
        },
        ColumnRef::BracketAccess(base, key) => match base.as_str() {
            "attributes" => lookup_attribute(&dp.attributes, key),
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

fn metric_type_name(metric: &Metric) -> String {
    match &metric.data {
        Some(metric::Data::Gauge(_)) => "gauge".to_string(),
        Some(metric::Data::Sum(_)) => "sum".to_string(),
        Some(metric::Data::Histogram(_)) => "histogram".to_string(),
        Some(metric::Data::ExponentialHistogram(_)) => "exponential_histogram".to_string(),
        Some(metric::Data::Summary(_)) => "summary".to_string(),
        None => "unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::opentelemetry::proto::{
        common::v1::{any_value, AnyValue, KeyValue},
        metrics::v1::{
            Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics,
            ScopeMetrics,
        },
        resource::v1::Resource,
    };
    use crate::query::sql::parser::SqlValue;
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

    fn make_gauge_metric(
        service_name: &str,
        metric_name: &str,
        value: f64,
        time_ns: u64,
        attrs: Vec<KeyValue>,
    ) -> ResourceMetrics {
        ResourceMetrics {
            resource: make_resource(service_name),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: attrs,
                            start_time_unix_nano: 0,
                            time_unix_nano: time_ns,
                            value: Some(number_data_point::Value::AsDouble(value)),
                            exemplars: vec![],
                            flags: 0,
                        }],
                    })),
                    metadata: vec![],
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }
    }

    fn make_query(where_expr: Option<WhereExpr>) -> SqlQuery {
        SqlQuery {
            table: TargetTable::Metrics,
            where_expr,
            limit: None,
            order_by: vec![],
            select_all: true,
        }
    }

    fn setup_store() -> Store {
        let (mut store, _rx) = Store::new(100);
        store.insert_metrics(vec![
            make_gauge_metric("frontend", "http.duration", 150.0, 1000, vec![]),
            make_gauge_metric(
                "backend",
                "db.latency",
                50.0,
                2000,
                vec![make_kv("db", "postgres")],
            ),
            make_gauge_metric("frontend", "http.duration", 200.0, 3000, vec![]),
            make_gauge_metric("backend", "cpu.usage", 75.0, 4000, vec![]),
        ]);
        store
    }

    #[test]
    fn eval_no_filter() {
        let store = setup_store();
        let query = make_query(None);
        let result = eval_metrics(&store, &query);
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn eval_filter_by_metric_name() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::Named("metric_name".to_string()),
            op: CompOp::Eq,
            value: SqlValue::String("http.duration".to_string()),
        }));
        let result = eval_metrics(&store, &query);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn eval_filter_by_service_name() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::Named("service_name".to_string()),
            op: CompOp::Eq,
            value: SqlValue::String("backend".to_string()),
        }));
        let result = eval_metrics(&store, &query);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn eval_filter_by_value() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::Named("value".to_string()),
            op: CompOp::Gt,
            value: SqlValue::Number(100.0),
        }));
        let result = eval_metrics(&store, &query);
        assert_eq!(result.len(), 2); // 150 and 200
    }

    #[test]
    fn eval_filter_by_type() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::Named("type".to_string()),
            op: CompOp::Eq,
            value: SqlValue::String("gauge".to_string()),
        }));
        let result = eval_metrics(&store, &query);
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn eval_filter_by_attribute() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::BracketAccess("attributes".to_string(), "db".to_string()),
            op: CompOp::Eq,
            value: SqlValue::String("postgres".to_string()),
        }));
        let result = eval_metrics(&store, &query);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn eval_with_limit() {
        let store = setup_store();
        let mut query = make_query(None);
        query.limit = Some(2);
        let result = eval_metrics(&store, &query);
        assert_eq!(result.len(), 2);
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
            Box::new(WhereExpr::Comparison {
                column: ColumnRef::Named("value".to_string()),
                op: CompOp::Gt,
                value: SqlValue::Number(100.0),
            }),
        )));
        let result = eval_metrics(&store, &query);
        assert_eq!(result.len(), 2); // both frontend metrics have value > 100 (150, 200)
    }

    #[test]
    fn eval_histogram_metric() {
        let (mut store, _rx) = Store::new(100);
        store.insert_metrics(vec![ResourceMetrics {
            resource: make_resource("svc"),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "request.duration".to_string(),
                    description: String::new(),
                    unit: String::new(),
                    data: Some(metric::Data::Histogram(Histogram {
                        data_points: vec![HistogramDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: 0,
                            time_unix_nano: 1000,
                            count: 100,
                            sum: Some(5000.0),
                            bucket_counts: vec![],
                            explicit_bounds: vec![],
                            exemplars: vec![],
                            flags: 0,
                            min: None,
                            max: None,
                        }],
                        aggregation_temporality: 0,
                    })),
                    metadata: vec![],
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }]);

        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::Named("type".to_string()),
            op: CompOp::Eq,
            value: SqlValue::String("histogram".to_string()),
        }));
        let result = eval_metrics(&store, &query);
        assert_eq!(result.len(), 1);

        // Also test count and sum columns
        let query = make_query(Some(WhereExpr::Comparison {
            column: ColumnRef::Named("count".to_string()),
            op: CompOp::Eq,
            value: SqlValue::Number(100.0),
        }));
        let result = eval_metrics(&store, &query);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn eval_metric_name_like() {
        let store = setup_store();
        let query = make_query(Some(WhereExpr::Like {
            column: ColumnRef::Named("metric_name".to_string()),
            pattern: "http%".to_string(),
            negated: false,
        }));
        let result = eval_metrics(&store, &query);
        assert_eq!(result.len(), 2);
    }
}
