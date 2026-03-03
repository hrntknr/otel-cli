use tonic::{Request, Response, Status};
use tracing::instrument;

use crate::proto::opentelemetry::proto::collector::{
    logs::v1::{
        logs_service_server::LogsService, ExportLogsServiceRequest, ExportLogsServiceResponse,
    },
    metrics::v1::{
        metrics_service_server::MetricsService, ExportMetricsServiceRequest,
        ExportMetricsServiceResponse,
    },
    trace::v1::{
        trace_service_server::TraceService, ExportTraceServiceRequest, ExportTraceServiceResponse,
    },
};
use crate::store::SharedStore;

pub struct OtlpGrpcService {
    store: SharedStore,
}

impl OtlpGrpcService {
    pub fn new(store: SharedStore) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl TraceService for OtlpGrpcService {
    #[instrument(name = "otlp.grpc.export_traces", skip_all, fields(resource_spans.count))]
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let msg = request.into_inner();
        let count = msg.resource_spans.len();
        tracing::Span::current().record("resource_spans.count", count);
        tracing::debug!(count, "received trace export via gRPC");
        let mut store = self.store.write().await;
        store.insert_traces(msg.resource_spans);
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl LogsService for OtlpGrpcService {
    #[instrument(name = "otlp.grpc.export_logs", skip_all, fields(resource_logs.count))]
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let msg = request.into_inner();
        let count = msg.resource_logs.len();
        tracing::Span::current().record("resource_logs.count", count);
        tracing::debug!(count, "received log export via gRPC");
        let mut store = self.store.write().await;
        store.insert_logs(msg.resource_logs);
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpGrpcService {
    #[instrument(name = "otlp.grpc.export_metrics", skip_all, fields(resource_metrics.count))]
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let msg = request.into_inner();
        let count = msg.resource_metrics.len();
        tracing::Span::current().record("resource_metrics.count", count);
        tracing::debug!(count, "received metric export via gRPC");
        let mut store = self.store.write().await;
        store.insert_metrics(msg.resource_metrics);
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}
