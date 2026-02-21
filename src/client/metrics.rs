use crate::cli::OutputFormat;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::QueryMetricsRequest;
use crate::proto::opentelemetry::proto::metrics::v1::{
    metric, number_data_point, ResourceMetrics,
};

use super::{format_attributes_json, format_timestamp, get_service_name};

pub async fn query_metrics(
    server: &str,
    service: Option<String>,
    name: Option<String>,
    limit: i32,
    format: &OutputFormat,
) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;

    let request = QueryMetricsRequest {
        service_name: service.unwrap_or_default(),
        metric_name: name.unwrap_or_default(),
        limit,
    };

    let response = client.query_metrics(request).await?.into_inner();

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

fn print_metrics_text(resource_metrics: &[ResourceMetrics]) {
    for rm in resource_metrics {
        let service_name = get_service_name(&rm.resource);
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                let type_name = metric_type_name(&m.data);
                println!(
                    "Metric: {} ({}) | Service: {}",
                    m.name, type_name, service_name
                );
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

fn print_metrics_json(resource_metrics: &[ResourceMetrics]) -> anyhow::Result<()> {
    let mut metrics = Vec::new();

    for rm in resource_metrics {
        let service_name = get_service_name(&rm.resource);
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                let type_name = metric_type_name(&m.data);
                let data_points = build_data_points_json(&m.data);

                let entry = serde_json::json!({
                    "name": m.name,
                    "type": type_name,
                    "service_name": service_name,
                    "data_points": data_points,
                });
                metrics.push(entry);
            }
        }
    }

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
