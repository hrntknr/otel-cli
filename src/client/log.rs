use crate::cli::OutputFormat;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::QueryLogsRequest;
use crate::proto::opentelemetry::proto::logs::v1::ResourceLogs;

use super::{
    extract_any_value_string, format_attributes_json, format_timestamp, get_service_name,
};

pub async fn query_logs(
    server: &str,
    service: Option<String>,
    severity: Option<String>,
    attributes: Vec<(String, String)>,
    limit: i32,
    format: &OutputFormat,
) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;

    let request = QueryLogsRequest {
        service_name: service.unwrap_or_default(),
        severity: severity.unwrap_or_default(),
        attributes: attributes.into_iter().collect(),
        limit,
    };

    let response = client.query_logs(request).await?.into_inner();

    match format {
        OutputFormat::Json => {
            print_logs_json(&response.resource_logs)?;
        }
        OutputFormat::Text => {
            print_logs_text(&response.resource_logs);
        }
    }

    Ok(())
}

fn print_logs_text(resource_logs: &[ResourceLogs]) {
    for rl in resource_logs {
        let service_name = get_service_name(&rl.resource);
        for sl in &rl.scope_logs {
            for lr in &sl.log_records {
                let timestamp = format_timestamp(lr.time_unix_nano);
                let severity = &lr.severity_text;
                let body = lr
                    .body
                    .as_ref()
                    .map(extract_any_value_string)
                    .unwrap_or_default();

                println!(
                    "{} [{}] {} | Service: {}",
                    timestamp, severity, body, service_name
                );
                if !lr.attributes.is_empty() {
                    println!("  Attributes:");
                    for kv in &lr.attributes {
                        let val = kv
                            .value
                            .as_ref()
                            .map(extract_any_value_string)
                            .unwrap_or_default();
                        println!("    {}: {}", kv.key, val);
                    }
                }
            }
        }
    }
}

fn print_logs_json(resource_logs: &[ResourceLogs]) -> anyhow::Result<()> {
    let mut logs = Vec::new();

    for rl in resource_logs {
        let service_name = get_service_name(&rl.resource);
        for sl in &rl.scope_logs {
            for lr in &sl.log_records {
                let body = lr
                    .body
                    .as_ref()
                    .map(extract_any_value_string)
                    .unwrap_or_default();

                let entry = serde_json::json!({
                    "timestamp": format_timestamp(lr.time_unix_nano),
                    "severity": lr.severity_text,
                    "body": body,
                    "service_name": service_name,
                    "attributes": format_attributes_json(&lr.attributes),
                });
                logs.push(entry);
            }
        }
    }

    println!("{}", serde_json::to_string_pretty(&logs)?);
    Ok(())
}
