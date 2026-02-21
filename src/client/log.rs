use crate::cli::OutputFormat;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::QueryLogsRequest;
use crate::proto::opentelemetry::proto::logs::v1::ResourceLogs;

use super::{
    extract_any_value_string, format_attributes_json, format_timestamp, get_resource_attributes,
};

/// Parse a time specification into nanoseconds since epoch.
///
/// Supported formats:
/// - Relative: `30s`, `5m`, `1h`, `2d` (interpreted as now - duration)
/// - Absolute: RFC3339 string like `2024-01-01T00:00:00Z`
pub fn parse_time_spec(s: &str) -> anyhow::Result<u64> {
    // Try relative duration first
    let s_trimmed = s.trim();
    if let Some(num_str) = s_trimmed.strip_suffix('s') {
        if let Ok(n) = num_str.parse::<u64>() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos() as u64;
            return Ok(now - n * 1_000_000_000);
        }
    }
    if let Some(num_str) = s_trimmed.strip_suffix('m') {
        if let Ok(n) = num_str.parse::<u64>() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos() as u64;
            return Ok(now - n * 60 * 1_000_000_000);
        }
    }
    if let Some(num_str) = s_trimmed.strip_suffix('h') {
        if let Ok(n) = num_str.parse::<u64>() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos() as u64;
            return Ok(now - n * 3600 * 1_000_000_000);
        }
    }
    if let Some(num_str) = s_trimmed.strip_suffix('d') {
        if let Ok(n) = num_str.parse::<u64>() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos() as u64;
            return Ok(now - n * 86400 * 1_000_000_000);
        }
    }

    // Try RFC3339
    let dt = chrono::DateTime::parse_from_rfc3339(s_trimmed)
        .map_err(|e| anyhow::anyhow!("invalid time spec '{}': {}", s, e))?;
    Ok(dt.timestamp_nanos_opt().ok_or_else(|| {
        anyhow::anyhow!("timestamp out of range: {}", s)
    })? as u64)
}

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

    let start_time_unix_nano = match since {
        Some(ref s) => parse_time_spec(s)?,
        None => 0,
    };
    let end_time_unix_nano = match until {
        Some(ref s) => parse_time_spec(s)?,
        None => 0,
    };

    let request = QueryLogsRequest {
        service_name: service.unwrap_or_default(),
        severity: severity.unwrap_or_default(),
        attributes: attributes.into_iter().collect(),
        limit,
        start_time_unix_nano,
        end_time_unix_nano,
    };

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

    let start_time_unix_nano = match since {
        Some(ref s) => parse_time_spec(s)?,
        None => 0,
    };
    let end_time_unix_nano = match until {
        Some(ref s) => parse_time_spec(s)?,
        None => 0,
    };

    let request = QueryLogsRequest {
        service_name: service.unwrap_or_default(),
        severity: severity.unwrap_or_default(),
        attributes: attributes.into_iter().collect(),
        limit,
        start_time_unix_nano,
        end_time_unix_nano,
    };

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_relative_seconds() {
        let result = parse_time_spec("30s").unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        // Should be approximately now - 30s (allow 1s tolerance)
        assert!(now - result > 29 * 1_000_000_000);
        assert!(now - result < 31 * 1_000_000_000);
    }

    #[test]
    fn parse_relative_minutes() {
        let result = parse_time_spec("5m").unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let diff = now - result;
        assert!(diff > 4 * 60 * 1_000_000_000);
        assert!(diff < 6 * 60 * 1_000_000_000);
    }

    #[test]
    fn parse_relative_hours() {
        let result = parse_time_spec("1h").unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let diff = now - result;
        assert!(diff > 59 * 60 * 1_000_000_000);
        assert!(diff < 61 * 60 * 1_000_000_000);
    }

    #[test]
    fn parse_relative_days() {
        let result = parse_time_spec("2d").unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let diff = now - result;
        assert!(diff > 47 * 3600 * 1_000_000_000);
        assert!(diff < 49 * 3600 * 1_000_000_000);
    }

    #[test]
    fn parse_rfc3339() {
        let result = parse_time_spec("2024-01-01T00:00:00Z").unwrap();
        assert_eq!(result, 1704067200_000_000_000);
    }

    #[test]
    fn parse_invalid() {
        assert!(parse_time_spec("invalid").is_err());
    }
}
