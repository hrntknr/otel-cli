pub mod sql;

#[derive(Debug, Clone)]
pub enum TargetTable {
    Traces,
    Logs,
    Metrics,
}

pub enum QueryResult {
    Traces(Vec<crate::store::TraceGroup>),
    Logs(Vec<crate::proto::opentelemetry::proto::logs::v1::ResourceLogs>),
    Metrics(Vec<crate::proto::opentelemetry::proto::metrics::v1::ResourceMetrics>),
}

pub type Row = Vec<(String, RowValue)>;

#[derive(Debug, Clone)]
pub enum RowValue {
    String(String),
    Int(i64),
    Double(f64),
    KeyValueList(Vec<crate::proto::opentelemetry::proto::common::v1::KeyValue>),
    Null,
}
