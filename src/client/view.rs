use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::{QueryLogsRequest, QueryMetricsRequest, QueryTracesRequest};
use crate::store;

pub async fn run_view(server: &str, max_items: usize) -> anyhow::Result<()> {
    let (store, event_rx) = store::new_shared(max_items);

    let mut client = QueryServiceClient::connect(server.to_string()).await?;

    let traces_store = store.clone();
    let mut traces_stream = client
        .follow_traces(QueryTracesRequest {
            delta: true,
            ..Default::default()
        })
        .await?
        .into_inner();
    tokio::spawn(async move {
        while let Ok(Some(msg)) = traces_stream.message().await {
            let resource_spans: Vec<_> = msg
                .trace_groups
                .into_iter()
                .flat_map(|g| g.resource_spans)
                .collect();
            if !resource_spans.is_empty() {
                traces_store.write().await.insert_traces(resource_spans);
            }
        }
    });

    let logs_store = store.clone();
    let mut logs_stream = client
        .follow_logs(QueryLogsRequest::default())
        .await?
        .into_inner();
    tokio::spawn(async move {
        while let Ok(Some(msg)) = logs_stream.message().await {
            if !msg.resource_logs.is_empty() {
                logs_store.write().await.insert_logs(msg.resource_logs);
            }
        }
    });

    let metrics_store = store.clone();
    let mut metrics_stream = client
        .follow_metrics(QueryMetricsRequest::default())
        .await?
        .into_inner();
    tokio::spawn(async move {
        while let Ok(Some(msg)) = metrics_stream.message().await {
            if !msg.resource_metrics.is_empty() {
                metrics_store
                    .write()
                    .await
                    .insert_metrics(msg.resource_metrics);
            }
        }
    });

    crate::tui::run(store, event_rx).await
}
