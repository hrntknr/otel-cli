pub mod arrow_convert;
pub mod arrow_schema;
pub mod datafusion_ctx;
pub mod sql;
pub mod table_provider;

pub type Row = Vec<(String, RowValue)>;

#[derive(Debug, Clone)]
pub enum RowValue {
    String(String),
    Int(i64),
    Double(f64),
    KeyValueList(Vec<crate::proto::opentelemetry::proto::common::v1::KeyValue>),
    Null,
}

pub fn severity_text_to_number(text: &str) -> i32 {
    match text.to_uppercase().as_str() {
        "TRACE" => 1,
        "DEBUG" => 5,
        "INFO" => 9,
        "WARN" => 13,
        "ERROR" => 17,
        "FATAL" => 21,
        _ => 0,
    }
}
