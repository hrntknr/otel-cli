pub mod clear;
pub mod log;
pub mod metrics;
pub mod shutdown;
pub mod sql;
pub mod status;
pub mod trace;
pub mod view;

use crate::proto::opentelemetry::proto::common::v1::{any_value, AnyValue, KeyValue};
use crate::proto::opentelemetry::proto::resource::v1::Resource;

pub fn hex_encode(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

pub fn format_timestamp(nanos: u64) -> String {
    if nanos == 0 {
        return "N/A".to_string();
    }
    let secs = (nanos / 1_000_000_000) as i64;
    let nsec = (nanos % 1_000_000_000) as u32;
    match chrono::DateTime::from_timestamp(secs, nsec) {
        Some(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        None => "N/A".to_string(),
    }
}

pub fn extract_any_value_string(value: &AnyValue) -> String {
    match &value.value {
        Some(any_value::Value::StringValue(s)) => s.clone(),
        Some(any_value::Value::BoolValue(b)) => b.to_string(),
        Some(any_value::Value::IntValue(i)) => i.to_string(),
        Some(any_value::Value::DoubleValue(d)) => d.to_string(),
        Some(any_value::Value::BytesValue(b)) => hex_encode(b),
        _ => String::new(),
    }
}

pub fn get_resource_attributes(resource: &Option<Resource>) -> &[KeyValue] {
    match resource {
        Some(r) => &r.attributes,
        None => &[],
    }
}

pub fn get_service_name(resource: &Option<Resource>) -> String {
    resource
        .as_ref()
        .and_then(|r| {
            r.attributes
                .iter()
                .find(|kv| kv.key == "service.name")
                .and_then(|kv| kv.value.as_ref())
                .and_then(|v| match &v.value {
                    Some(any_value::Value::StringValue(s)) => Some(s.clone()),
                    _ => None,
                })
        })
        .unwrap_or_default()
}

/// Parse a time specification into nanoseconds since epoch.
///
/// Supported formats:
/// - Relative: `30s`, `5m`, `1h`, `2d` (interpreted as now - duration)
/// - Absolute: RFC3339 string like `2024-01-01T00:00:00Z`
pub fn parse_time_spec(s: &str) -> anyhow::Result<u64> {
    let s_trimmed = s.trim();

    // Try relative duration first
    const UNITS: &[(char, u64)] = &[('s', 1), ('m', 60), ('h', 3600), ('d', 86400)];
    for &(suffix, multiplier) in UNITS {
        if let Some(num_str) = s_trimmed.strip_suffix(suffix) {
            if let Ok(n) = num_str.parse::<u64>() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_nanos() as u64;
                return Ok(now - n * multiplier * 1_000_000_000);
            }
        }
    }

    // Try RFC3339
    let dt = chrono::DateTime::parse_from_rfc3339(s_trimmed)
        .map_err(|e| anyhow::anyhow!("invalid time spec '{}': {}", s, e))?;
    Ok(dt
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow::anyhow!("timestamp out of range: {}", s))? as u64)
}

// --- Query + format helpers ---

use crate::cli::OutputFormat;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::Row as ProtoRow;
use crate::proto::otelcli::query::v1::SqlQueryRequest;

pub async fn query_and_print(
    server: &str,
    sql: &str,
    format: &OutputFormat,
    print_text: fn(&[ProtoRow]),
) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let response = client
        .sql_query(SqlQueryRequest {
            query: sql.to_string(),
        })
        .await?
        .into_inner();
    print_rows(&response.rows, format, print_text)
}

pub async fn follow_and_print(
    server: &str,
    sql: &str,
    format: &OutputFormat,
    print_text: fn(&[ProtoRow]),
) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let mut stream = client
        .follow_sql(SqlQueryRequest {
            query: sql.to_string(),
        })
        .await?
        .into_inner();
    let mut csv_header_shown = false;
    while let Some(msg) = stream.message().await? {
        match format {
            OutputFormat::Jsonl => print_rows_jsonl(&msg.rows)?,
            OutputFormat::Csv => {
                print_rows_csv(&msg.rows, !csv_header_shown);
                csv_header_shown = true;
            }
            OutputFormat::Table => print_rows_table(&msg.rows),
            OutputFormat::Text => print_text(&msg.rows),
        }
    }
    Ok(())
}

fn print_rows(
    rows: &[ProtoRow],
    format: &OutputFormat,
    print_text: fn(&[ProtoRow]),
) -> anyhow::Result<()> {
    match format {
        OutputFormat::Jsonl => print_rows_jsonl(rows)?,
        OutputFormat::Csv => print_rows_csv(rows, true),
        OutputFormat::Table => print_rows_table(rows),
        OutputFormat::Text => print_text(rows),
    }
    Ok(())
}

// --- Row display helpers ---

pub fn get_row_string(row: &ProtoRow, name: &str) -> Option<String> {
    row.columns.iter().find(|c| c.name == name).and_then(|c| {
        c.value
            .as_ref()
            .map(extract_any_value_string)
            .filter(|s| !s.is_empty())
    })
}

/// Get a timestamp column and format it as human-readable RFC3339.
pub fn get_row_timestamp(row: &ProtoRow, name: &str) -> Option<String> {
    row.columns.iter().find(|c| c.name == name).and_then(|c| {
        c.value
            .as_ref()
            .map(format_timestamp_value)
            .filter(|s| !s.is_empty())
    })
}

pub fn get_row_kvlist<'a>(row: &'a ProtoRow, name: &str) -> Option<&'a [KeyValue]> {
    row.columns.iter().find(|c| c.name == name).and_then(|c| {
        c.value.as_ref().and_then(|v| match &v.value {
            Some(any_value::Value::KvlistValue(kvl)) => Some(kvl.values.as_slice()),
            _ => None,
        })
    })
}

pub fn print_kvlist(kvs: &[KeyValue], label: &str, indent: &str) {
    if !kvs.is_empty() {
        println!("{}{}:", indent, label);
        for kv in kvs {
            let val = kv
                .value
                .as_ref()
                .map(extract_any_value_string)
                .unwrap_or_default();
            println!("{}  {}: {}", indent, kv.key, val);
        }
    }
}

pub fn row_to_json(row: &ProtoRow) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for col in &row.columns {
        let value = match &col.value {
            Some(v) => any_value_to_json(v),
            None => serde_json::Value::Null,
        };
        map.insert(col.name.clone(), value);
    }
    serde_json::Value::Object(map)
}

pub fn print_rows_jsonl(rows: &[ProtoRow]) -> anyhow::Result<()> {
    for row in rows {
        println!("{}", serde_json::to_string(&row_to_json(row))?);
    }
    Ok(())
}

pub fn print_rows_csv(rows: &[ProtoRow], show_header: bool) {
    if rows.is_empty() {
        return;
    }
    if show_header {
        let headers: Vec<&str> = rows[0].columns.iter().map(|c| c.name.as_str()).collect();
        println!("{}", headers.join(","));
    }
    for row in rows {
        let cells: Vec<String> = row
            .columns
            .iter()
            .map(|c| match &c.value {
                Some(v) => csv_escape(&format_any_value_for_csv(v)),
                None => String::new(),
            })
            .collect();
        println!("{}", cells.join(","));
    }
}

pub fn print_rows_table(rows: &[ProtoRow]) {
    if rows.is_empty() {
        return;
    }

    let headers: Vec<&str> = rows[0].columns.iter().map(|c| c.name.as_str()).collect();

    // Format all cell values
    let formatted: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.columns
                .iter()
                .map(|c| match &c.value {
                    Some(v) => {
                        if is_timestamp_column(&c.name) {
                            format_timestamp_value(v)
                        } else {
                            format_any_value_for_table(v)
                        }
                    }
                    None => "NULL".to_string(),
                })
                .collect()
        })
        .collect();

    // Calculate column widths
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in &formatted {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    // Print header
    let header_line: Vec<String> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| format!("{:width$}", h, width = widths[i]))
        .collect();
    println!("{}", header_line.join(" | "));

    // Print separator
    let sep_line: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep_line.join("-+-"));

    // Print rows
    for row in &formatted {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, cell)| {
                let w = widths.get(i).copied().unwrap_or(0);
                format!("{:width$}", cell, width = w)
            })
            .collect();
        println!("{}", cells.join(" | "));
    }
}

fn is_timestamp_column(name: &str) -> bool {
    matches!(
        name,
        "timestamp" | "start_time" | "end_time" | "time_unix_nano"
    )
}

fn format_timestamp_value(value: &AnyValue) -> String {
    match &value.value {
        Some(any_value::Value::IntValue(nanos)) => format_timestamp(*nanos as u64),
        Some(any_value::Value::StringValue(s)) => {
            if let Ok(nanos) = s.parse::<u64>() {
                format_timestamp(nanos)
            } else {
                s.clone()
            }
        }
        _ => extract_any_value_string(value),
    }
}

fn format_any_value_for_table(value: &AnyValue) -> String {
    match &value.value {
        Some(any_value::Value::KvlistValue(kvl)) => {
            let pairs: Vec<String> = kvl
                .values
                .iter()
                .map(|kv| {
                    let val = kv
                        .value
                        .as_ref()
                        .map(extract_any_value_string)
                        .unwrap_or_default();
                    format!("{}={}", kv.key, val)
                })
                .collect();
            format!("{{{}}}", pairs.join(", "))
        }
        _ => extract_any_value_string(value),
    }
}

fn format_any_value_for_csv(value: &AnyValue) -> String {
    match &value.value {
        Some(any_value::Value::KvlistValue(kvl)) => kvl
            .values
            .iter()
            .map(|kv| {
                let val = kv
                    .value
                    .as_ref()
                    .map(extract_any_value_string)
                    .unwrap_or_default();
                format!("{}={}", kv.key, val)
            })
            .collect::<Vec<_>>()
            .join(", "),
        _ => extract_any_value_string(value),
    }
}

fn csv_escape(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

pub fn format_attributes_json(attributes: &[KeyValue]) -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = attributes
        .iter()
        .filter_map(|kv| {
            let val = kv.value.as_ref().map(any_value_to_json);
            val.map(|v| (kv.key.clone(), v))
        })
        .collect();
    serde_json::Value::Object(map)
}

pub(crate) fn any_value_to_json(value: &AnyValue) -> serde_json::Value {
    match &value.value {
        Some(any_value::Value::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(any_value::Value::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(any_value::Value::IntValue(i)) => serde_json::json!(*i),
        Some(any_value::Value::DoubleValue(d)) => serde_json::json!(*d),
        Some(any_value::Value::BytesValue(b)) => serde_json::Value::String(hex_encode(b)),
        Some(any_value::Value::ArrayValue(arr)) => {
            serde_json::Value::Array(arr.values.iter().map(any_value_to_json).collect())
        }
        Some(any_value::Value::KvlistValue(kvl)) => format_attributes_json(&kvl.values),
        None => serde_json::Value::Null,
    }
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
        assert_eq!(result, 1_704_067_200_000_000_000);
    }

    #[test]
    fn parse_invalid() {
        assert!(parse_time_spec("invalid").is_err());
    }
}
