pub mod otlp_grpc;
pub mod otlp_http;
pub mod query_grpc;

use std::sync::Arc;

use crate::proto::opentelemetry::proto::collector::{
    logs::v1::logs_service_server::LogsServiceServer,
    metrics::v1::metrics_service_server::MetricsServiceServer,
    trace::v1::trace_service_server::TraceServiceServer,
};
use crate::proto::otelcli::query::v1::query_service_server::QueryServiceServer;
use crate::store::SharedStore;
use tokio_util::sync::CancellationToken;

pub async fn run_grpc_server(
    addr: std::net::SocketAddr,
    store: SharedStore,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let otlp_service = Arc::new(otlp_grpc::OtlpGrpcService::new(store.clone()));
    let query_service = query_grpc::QueryGrpcService::new(store);

    tonic::transport::Server::builder()
        .add_service(TraceServiceServer::from_arc(otlp_service.clone()))
        .add_service(LogsServiceServer::from_arc(otlp_service.clone()))
        .add_service(MetricsServiceServer::from_arc(otlp_service))
        .add_service(QueryServiceServer::new(query_service))
        .serve_with_shutdown(addr, shutdown.cancelled())
        .await?;

    Ok(())
}

pub async fn run_http_server(
    addr: std::net::SocketAddr,
    store: SharedStore,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let app = otlp_http::router(store);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}
