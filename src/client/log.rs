use crate::cli::OutputFormat;
use crate::proto::opentelemetry::proto::logs::v1::ResourceLogs;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::SqlQueryRequest;
use crate::query::sql::convert::log_flags_to_sql;

use super::{
    extract_any_value_string, format_attributes_json, format_timestamp, get_resource_attributes,
    parse_time_spec,
};

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
    let start_time_ns = since.as_deref().map(parse_time_spec).transpose()?;
    let end_time_ns = until.as_deref().map(parse_time_spec).transpose()?;
    let sql = log_flags_to_sql(
        service.as_deref(),
        severity.as_deref(),
        &attributes,
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
            print_logs_json(&response.resource_logs)?;
        }
        OutputFormat::Text => {
            print_logs_text(&response.resource_logs);
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
    let start_time_ns = since.as_deref().map(parse_time_spec).transpose()?;
    let end_time_ns = until.as_deref().map(parse_time_spec).transpose()?;
    let sql = log_flags_to_sql(
        service.as_deref(),
        severity.as_deref(),
        &attributes,
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
                print_logs_json(&msg.resource_logs)?;
            }
            OutputFormat::Text => {
                print_logs_text(&msg.resource_logs);
            }
        }
    }

    Ok(())
}

pub fn print_logs_text(resource_logs: &[ResourceLogs]) {
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

pub fn print_logs_json(resource_logs: &[ResourceLogs]) -> anyhow::Result<()> {
    let logs = build_logs_value(resource_logs);
    println!("{}", serde_json::to_string_pretty(&logs)?);
    Ok(())
}
