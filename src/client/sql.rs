use crate::cli::OutputFormat;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::SqlQueryRequest;

pub async fn query_sql(server: &str, query: &str, format: &OutputFormat) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let response = client
        .sql_query(SqlQueryRequest {
            query: query.to_string(),
        })
        .await?
        .into_inner();

    if !response.trace_groups.is_empty() {
        match format {
            OutputFormat::Json => super::trace::print_traces_json(&response.trace_groups)?,
            OutputFormat::Text => super::trace::print_traces_text(&response.trace_groups),
        }
    }
    if !response.resource_logs.is_empty() {
        match format {
            OutputFormat::Json => super::log::print_logs_json(&response.resource_logs)?,
            OutputFormat::Text => super::log::print_logs_text(&response.resource_logs),
        }
    }
    if !response.resource_metrics.is_empty() {
        match format {
            OutputFormat::Json => super::metrics::print_metrics_json(&response.resource_metrics)?,
            OutputFormat::Text => super::metrics::print_metrics_text(&response.resource_metrics),
        }
    }
    Ok(())
}

pub async fn follow_sql(server: &str, query: &str, format: &OutputFormat) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let mut stream = client
        .follow_sql(SqlQueryRequest {
            query: query.to_string(),
        })
        .await?
        .into_inner();

    while let Some(msg) = stream.message().await? {
        if !msg.trace_groups.is_empty() {
            match format {
                OutputFormat::Json => super::trace::print_traces_json(&msg.trace_groups)?,
                OutputFormat::Text => super::trace::print_traces_text(&msg.trace_groups),
            }
        }
        if !msg.resource_logs.is_empty() {
            match format {
                OutputFormat::Json => super::log::print_logs_json(&msg.resource_logs)?,
                OutputFormat::Text => super::log::print_logs_text(&msg.resource_logs),
            }
        }
        if !msg.resource_metrics.is_empty() {
            match format {
                OutputFormat::Json => super::metrics::print_metrics_json(&msg.resource_metrics)?,
                OutputFormat::Text => super::metrics::print_metrics_text(&msg.resource_metrics),
            }
        }
    }
    Ok(())
}
