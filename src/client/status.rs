use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::StatusRequest;

pub async fn status(server: &str) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let resp = client.status(StatusRequest {}).await?.into_inner();

    println!("Traces:  {}", resp.trace_count);
    println!("Logs:    {}", resp.log_count);
    println!("Metrics: {}", resp.metric_count);

    Ok(())
}
