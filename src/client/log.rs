use crate::cli::OutputFormat;
use crate::proto::opentelemetry::proto::logs::v1::ResourceLogs;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::QueryLogsRequest;

use super::{
    extract_any_value_string, format_attributes_json, format_timestamp, get_resource_attributes,
};

use super::parse_time_spec;

fn build_query_request(
    service: Option<String>,
    severity: Option<String>,
    attributes: Vec<(String, String)>,
    limit: i32,
    since: Option<String>,
    until: Option<String>,
) -> anyhow::Result<QueryLogsRequest> {
    let start_time_unix_nano = match since {
        Some(ref s) => parse_time_spec(s)?,
        None => 0,
    };
    let end_time_unix_nano = match until {
        Some(ref s) => parse_time_spec(s)?,
        None => 0,
    };
    Ok(QueryLogsRequest {
        service_name: service.unwrap_or_default(),
        severity: severity.unwrap_or_default(),
        attributes: attributes.into_iter().collect(),
        limit,
        start_time_unix_nano,
        end_time_unix_nano,
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn query_logs(
    server: &str,
    service: Option<String>,
    severity: Option<String>,
    attributes: Vec<(String, String)>,
    limit: i32,
    format: &OutputFormat,
    since: Option<String>,
    until: Option<String>,
) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let request = build_query_request(service, severity, attributes, limit, since, until)?;
    let response = client.query_logs(request).await?.into_inner();

    match format {
        OutputFormat::Json => {
            print_logs_json(&response.resource_logs)?;
        }
        OutputFormat::Text => {
            print_logs_text(&response.resource_logs);
        }
        OutputFormat::Toon => {
            print_logs_toon(&response.resource_logs)?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn follow_logs(
    server: &str,
    service: Option<String>,
    severity: Option<String>,
    attributes: Vec<(String, String)>,
    limit: i32,
    format: &OutputFormat,
    since: Option<String>,
    until: Option<String>,
) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let request = build_query_request(service, severity, attributes, limit, since, until)?;
    let mut stream = client.follow_logs(request).await?.into_inner();

    while let Some(msg) = stream.message().await? {
        match format {
            OutputFormat::Json => {
                print_logs_json(&msg.resource_logs)?;
            }
            OutputFormat::Text => {
                print_logs_text(&msg.resource_logs);
            }
            OutputFormat::Toon => {
                print_logs_toon(&msg.resource_logs)?;
            }
        }
    }

    Ok(())
}

fn print_logs_text(resource_logs: &[ResourceLogs]) {
    for rl in resource_logs {
        let resource_attrs = get_resource_attributes(&rl.resource);
        for sl in &rl.scope_logs {
            for lr in &sl.log_records {
                let timestamp = format_timestamp(lr.time_unix_nano);
                let severity = &lr.severity_text;
                let body = lr
                    .body
                    .as_ref()
                    .map(extract_any_value_string)
                    .unwrap_or_default();

                println!("{} [{}] {}", timestamp, severity, body);
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

fn build_logs_value(resource_logs: &[ResourceLogs]) -> Vec<serde_json::Value> {
    let mut logs = Vec::new();

    for rl in resource_logs {
        let resource_attrs = get_resource_attributes(&rl.resource);
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
                    "resource_attributes": format_attributes_json(resource_attrs),
                    "attributes": format_attributes_json(&lr.attributes),
                });
                logs.push(entry);
            }
        }
    }

    logs
}

fn print_logs_json(resource_logs: &[ResourceLogs]) -> anyhow::Result<()> {
    let logs = build_logs_value(resource_logs);
    println!("{}", serde_json::to_string_pretty(&logs)?);
    Ok(())
}

fn print_logs_toon(resource_logs: &[ResourceLogs]) -> anyhow::Result<()> {
    let logs = build_logs_value(resource_logs);
    println!("{}", toon_format::encode_default(&serde_json::json!(logs))?);
    Ok(())
}
