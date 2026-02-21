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
    listener: tokio::net::TcpListener,
    store: SharedStore,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let otlp_service = Arc::new(otlp_grpc::OtlpGrpcService::new(store));

    let incoming = tonic::transport::server::TcpIncoming::from(listener);
    tonic::transport::Server::builder()
        .add_service(TraceServiceServer::from_arc(otlp_service.clone()))
        .add_service(LogsServiceServer::from_arc(otlp_service.clone()))
        .add_service(MetricsServiceServer::from_arc(otlp_service))
        .serve_with_incoming_shutdown(incoming, shutdown.cancelled())
        .await?;

    Ok(())
}

pub async fn run_query_server(
    listener: tokio::net::TcpListener,
    store: SharedStore,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let query_service = query_grpc::QueryGrpcService::new(store);

    let incoming = tonic::transport::server::TcpIncoming::from(listener);
    tonic::transport::Server::builder()
        .add_service(QueryServiceServer::new(query_service))
        .serve_with_incoming_shutdown(incoming, shutdown.cancelled())
        .await?;

    Ok(())
}

pub async fn run_http_server(
    listener: tokio::net::TcpListener,
    store: SharedStore,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let app = otlp_http::router(store);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}

/// Bind TCP listeners for all ports upfront, returning an error if any port is in use.
pub async fn bind_listeners(
    grpc_addr: std::net::SocketAddr,
    http_addr: std::net::SocketAddr,
    query_addr: std::net::SocketAddr,
) -> anyhow::Result<(
    tokio::net::TcpListener,
    tokio::net::TcpListener,
    tokio::net::TcpListener,
)> {
    let grpc_listener = tokio::net::TcpListener::bind(grpc_addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind gRPC address {}: {}", grpc_addr, e))?;
    let http_listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind HTTP address {}: {}", http_addr, e))?;
    let query_listener = tokio::net::TcpListener::bind(query_addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind query address {}: {}", query_addr, e))?;
    Ok((grpc_listener, http_listener, query_listener))
}
