pub mod clear;
pub mod log;
pub mod metrics;
pub mod trace;

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

fn any_value_to_json(value: &AnyValue) -> serde_json::Value {
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
