use tonic::{Request, Response, Status};

use crate::proto::otelcli::query::v1::{
    query_service_server::QueryService as QueryServiceTrait, QueryLogsRequest, QueryLogsResponse,
    QueryMetricsRequest, QueryMetricsResponse, QueryTracesRequest, QueryTracesResponse,
};
use crate::store::{LogFilter, MetricFilter, SharedStore, TraceFilter};

pub struct QueryGrpcService {
    store: SharedStore,
}

impl QueryGrpcService {
    pub fn new(store: SharedStore) -> Self {
        Self { store }
    }
}

/// Convert an empty string to None, non-empty to Some.
fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Default limit when 0 or unset.
fn effective_limit(limit: i32) -> usize {
    if limit <= 0 { 100 } else { limit as usize }
}

#[tonic::async_trait]
impl QueryServiceTrait for QueryGrpcService {
    async fn query_traces(
        &self,
        request: Request<QueryTracesRequest>,
    ) -> Result<Response<QueryTracesResponse>, Status> {
        let req = request.into_inner();
        let filter = TraceFilter {
            service_name: non_empty(&req.service_name),
            trace_id: non_empty(&req.trace_id),
            attributes: req.attributes.into_iter().collect(),
        };
        let limit = effective_limit(req.limit);
        let store = self.store.read().await;
        let resource_spans = store.query_traces(&filter, limit);
        Ok(Response::new(QueryTracesResponse { resource_spans }))
    }

    async fn query_logs(
        &self,
        request: Request<QueryLogsRequest>,
    ) -> Result<Response<QueryLogsResponse>, Status> {
        let req = request.into_inner();
        let filter = LogFilter {
            service_name: non_empty(&req.service_name),
            severity: non_empty(&req.severity),
            attributes: req.attributes.into_iter().collect(),
        };
        let limit = effective_limit(req.limit);
        let store = self.store.read().await;
        let resource_logs = store.query_logs(&filter, limit);
        Ok(Response::new(QueryLogsResponse { resource_logs }))
    }

    async fn query_metrics(
        &self,
        request: Request<QueryMetricsRequest>,
    ) -> Result<Response<QueryMetricsResponse>, Status> {
        let req = request.into_inner();
        let filter = MetricFilter {
            service_name: non_empty(&req.service_name),
            metric_name: non_empty(&req.metric_name),
        };
        let limit = effective_limit(req.limit);
        let store = self.store.read().await;
        let resource_metrics = store.query_metrics(&filter, limit);
        Ok(Response::new(QueryMetricsResponse { resource_metrics }))
    }
}
