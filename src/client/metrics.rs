use crate::cli::OutputFormat;
use crate::proto::opentelemetry::proto::metrics::v1::{metric, number_data_point, ResourceMetrics};
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::SqlQueryRequest;
use crate::query::sql::convert::metric_flags_to_sql;

use super::{
    extract_any_value_string, format_attributes_json, format_timestamp, get_resource_attributes,
    parse_time_spec,
};

pub async fn query_metrics(
    server: &str,
    service: Option<String>,
    name: Option<String>,
    limit: i32,
    format: &OutputFormat,
    since: Option<String>,
    until: Option<String>,
) -> anyhow::Result<()> {
    let start_time_ns = since.as_deref().map(parse_time_spec).transpose()?;
    let end_time_ns = until.as_deref().map(parse_time_spec).transpose()?;
    let sql = metric_flags_to_sql(
        service.as_deref(),
        name.as_deref(),
        Some(limit as usize),
        start_time_ns,
        end_time_ns,
    );

    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let response = client
        .sql_query(SqlQueryRequest { query: sql })
        .await?
        .into_inner();

    match format {
        OutputFormat::Json => {
            print_metrics_json(&response.resource_metrics)?;
        }
        OutputFormat::Text => {
            print_metrics_text(&response.resource_metrics);
        }
    }

    Ok(())
}

pub async fn follow_metrics(
    server: &str,
    service: Option<String>,
    name: Option<String>,
    limit: i32,
    format: &OutputFormat,
    since: Option<String>,
    until: Option<String>,
) -> anyhow::Result<()> {
    let start_time_ns = since.as_deref().map(parse_time_spec).transpose()?;
    let end_time_ns = until.as_deref().map(parse_time_spec).transpose()?;
    let sql = metric_flags_to_sql(
        service.as_deref(),
        name.as_deref(),
        Some(limit as usize),
        start_time_ns,
        end_time_ns,
    );

    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let mut stream = client
        .follow_sql(SqlQueryRequest { query: sql })
        .await?
        .into_inner();

    while let Some(msg) = stream.message().await? {
        match format {
            OutputFormat::Json => {
                print_metrics_json(&msg.resource_metrics)?;
            }
            OutputFormat::Text => {
                print_metrics_text(&msg.resource_metrics);
            }
        }
    }

    Ok(())
}

fn metric_type_name(data: &Option<metric::Data>) -> &'static str {
    match data {
        Some(metric::Data::Gauge(_)) => "Gauge",
        Some(metric::Data::Sum(_)) => "Sum",
        Some(metric::Data::Histogram(_)) => "Histogram",
        Some(metric::Data::ExponentialHistogram(_)) => "ExponentialHistogram",
        Some(metric::Data::Summary(_)) => "Summary",
        None => "Unknown",
    }
}

fn format_number_value(value: &Option<number_data_point::Value>) -> String {
    match value {
        Some(number_data_point::Value::AsDouble(d)) => d.to_string(),
        Some(number_data_point::Value::AsInt(i)) => i.to_string(),
        None => "N/A".to_string(),
    }
}

pub fn print_metrics_text(resource_metrics: &[ResourceMetrics]) {
    for rm in resource_metrics {
        let resource_attrs = get_resource_attributes(&rm.resource);
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                let type_name = metric_type_name(&m.data);
                println!("Metric: {} ({})", m.name, type_name);
                if !resource_attrs.is_empty() {
                    println!("  Resource:");
                    for kv in resource_attrs {
                        let val = kv
                            .value
                            .as_ref()
                            .map(extract_any_value_string)
                            .unwrap_or_default();
                        println!("    {}: {}", kv.key, val);
                    }
                }
                println!("  Data points:");
                match &m.data {
                    Some(metric::Data::Gauge(g)) => {
                        for dp in &g.data_points {
                            println!(
                                "    Value: {} Time: {}",
                                format_number_value(&dp.value),
                                format_timestamp(dp.time_unix_nano)
                            );
                        }
                    }
                    Some(metric::Data::Sum(s)) => {
                        for dp in &s.data_points {
                            println!(
                                "    Value: {} Time: {}",
                                format_number_value(&dp.value),
                                format_timestamp(dp.time_unix_nano)
                            );
                        }
                    }
                    Some(metric::Data::Histogram(h)) => {
                        for dp in &h.data_points {
                            println!(
                                "    Count: {} Sum: {} Time: {}",
                                dp.count,
                                dp.sum.map(|v| v.to_string()).unwrap_or("N/A".into()),
                                format_timestamp(dp.time_unix_nano)
                            );
                        }
                    }
                    Some(metric::Data::ExponentialHistogram(eh)) => {
                        for dp in &eh.data_points {
                            println!(
                                "    Count: {} Sum: {} Time: {}",
                                dp.count,
                                dp.sum.map(|v| v.to_string()).unwrap_or("N/A".into()),
                                format_timestamp(dp.time_unix_nano)
                            );
                        }
                    }
                    Some(metric::Data::Summary(s)) => {
                        for dp in &s.data_points {
                            println!(
                                "    Count: {} Sum: {} Time: {}",
                                dp.count,
                                dp.sum,
                                format_timestamp(dp.time_unix_nano)
                            );
                        }
                    }
                    None => {
                        println!("    (no data)");
                    }
                }
            }
        }
    }
}

fn build_metrics_value(resource_metrics: &[ResourceMetrics]) -> Vec<serde_json::Value> {
    let mut metrics = Vec::new();

    for rm in resource_metrics {
        let resource_attrs = get_resource_attributes(&rm.resource);
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                let type_name = metric_type_name(&m.data);
                let data_points = build_data_points_json(&m.data);

                let entry = serde_json::json!({
                    "name": m.name,
                    "type": type_name,
                    "resource_attributes": format_attributes_json(resource_attrs),
                    "data_points": data_points,
                });
                metrics.push(entry);
            }
        }
    }

    metrics
}

pub fn print_metrics_json(resource_metrics: &[ResourceMetrics]) -> anyhow::Result<()> {
    let metrics = build_metrics_value(resource_metrics);
    println!("{}", serde_json::to_string_pretty(&metrics)?);
    Ok(())
}

fn build_data_points_json(data: &Option<metric::Data>) -> serde_json::Value {
    match data {
        Some(metric::Data::Gauge(g)) => {
            let points: Vec<_> = g
                .data_points
                .iter()
                .map(|dp| {
                    serde_json::json!({
                        "value": format_number_value(&dp.value),
                        "time": format_timestamp(dp.time_unix_nano),
                        "attributes": format_attributes_json(&dp.attributes),
                    })
                })
                .collect();
            serde_json::json!(points)
        }
        Some(metric::Data::Sum(s)) => {
            let points: Vec<_> = s
                .data_points
                .iter()
                .map(|dp| {
                    serde_json::json!({
                        "value": format_number_value(&dp.value),
                        "time": format_timestamp(dp.time_unix_nano),
                        "attributes": format_attributes_json(&dp.attributes),
                    })
                })
                .collect();
            serde_json::json!(points)
        }
        Some(metric::Data::Histogram(h)) => {
            let points: Vec<_> = h
                .data_points
                .iter()
                .map(|dp| {
                    serde_json::json!({
                        "count": dp.count,
                        "sum": dp.sum,
                        "time": format_timestamp(dp.time_unix_nano),
                        "attributes": format_attributes_json(&dp.attributes),
                    })
                })
                .collect();
            serde_json::json!(points)
        }
        Some(metric::Data::ExponentialHistogram(eh)) => {
            let points: Vec<_> = eh
                .data_points
                .iter()
                .map(|dp| {
                    serde_json::json!({
                        "count": dp.count,
                        "sum": dp.sum,
                        "time": format_timestamp(dp.time_unix_nano),
                        "attributes": format_attributes_json(&dp.attributes),
                    })
                })
                .collect();
            serde_json::json!(points)
        }
        Some(metric::Data::Summary(s)) => {
            let points: Vec<_> = s
                .data_points
                .iter()
                .map(|dp| {
                    serde_json::json!({
                        "count": dp.count,
                        "sum": dp.sum,
                        "time": format_timestamp(dp.time_unix_nano),
                        "attributes": format_attributes_json(&dp.attributes),
                    })
                })
                .collect();
            serde_json::json!(points)
        }
        None => serde_json::json!([]),
    }
}
