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
