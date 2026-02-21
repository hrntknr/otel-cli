use tonic::{Request, Response, Status};

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
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let msg = request.into_inner();
        let mut store = self.store.write().await;
        store.insert_traces(msg.resource_spans);
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl LogsService for OtlpGrpcService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let msg = request.into_inner();
        let mut store = self.store.write().await;
        store.insert_logs(msg.resource_logs);
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpGrpcService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let msg = request.into_inner();
        let mut store = self.store.write().await;
        store.insert_metrics(msg.resource_metrics);
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}
