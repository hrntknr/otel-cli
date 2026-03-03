use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::ShutdownRequest;

pub async fn shutdown(server: &str) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    client.shutdown(ShutdownRequest {}).await?;

    println!("Server shutdown initiated.");

    Ok(())
}
