pub mod clear;
pub mod log;
pub mod metrics;
pub mod sql;
pub mod trace;
pub mod view;

use crate::proto::opentelemetry::proto::common::v1::{any_value, AnyValue, KeyValue};
use crate::proto::opentelemetry::proto::resource::v1::Resource;

pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
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

// --- Row display helpers ---

use crate::proto::otelcli::query::v1::Row as ProtoRow;

pub fn get_row_string(row: &ProtoRow, name: &str) -> Option<String> {
    row.columns.iter().find(|c| c.name == name).and_then(|c| {
        c.value
            .as_ref()
            .map(|v| extract_any_value_string(v))
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
