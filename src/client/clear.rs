use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::{ClearLogsRequest, ClearMetricsRequest, ClearTracesRequest};

pub async fn clear(server: &str, traces: bool, logs: bool, metrics: bool) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;

    if traces {
        client.clear_traces(ClearTracesRequest {}).await?;
        println!("Traces cleared.");
    }
    if logs {
        client.clear_logs(ClearLogsRequest {}).await?;
        println!("Logs cleared.");
    }
    if metrics {
        client.clear_metrics(ClearMetricsRequest {}).await?;
        println!("Metrics cleared.");
    }

    if !traces && !logs && !metrics {
        println!("No target specified. Use --traces, --logs, and/or --metrics.");
    }

    Ok(())
}
