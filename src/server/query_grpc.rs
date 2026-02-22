use std::pin::Pin;

use tokio_stream::{wrappers::BroadcastStream, Stream, StreamExt};
use tonic::{Request, Response, Status};

use crate::proto::otelcli::query::v1::{
    query_service_server::QueryService as QueryServiceTrait, ClearLogsRequest, ClearMetricsRequest,
    ClearResponse, ClearTracesRequest, QueryLogsRequest, QueryLogsResponse, QueryMetricsRequest,
    QueryMetricsResponse, QueryTracesRequest, QueryTracesResponse,
};
use crate::store::{
    FilterCondition, FilterOperator, LogFilter, MetricFilter, SeverityCondition, SharedStore,
    StoreEvent, TraceFilter,
};

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
    if limit <= 0 {
        100
    } else {
        limit as usize
    }
}

fn build_log_filter(req: &QueryLogsRequest) -> LogFilter {
    let mut filter = LogFilter::default();
    filter.service_name = non_empty(&req.service_name);
    if let Some(sev) = non_empty(&req.severity) {
        filter.severity = Some(SeverityCondition {
            operator: FilterOperator::Ge,
            value: sev,
        });
    }
    for (k, v) in &req.attributes {
        filter.attribute_conditions.push(FilterCondition {
            field: k.clone(),
            operator: FilterOperator::Eq,
            value: v.clone(),
        });
    }
    if req.start_time_unix_nano != 0 {
        filter.start_time_ns = Some(req.start_time_unix_nano);
    }
    if req.end_time_unix_nano != 0 {
        filter.end_time_ns = Some(req.end_time_unix_nano);
    }
    filter
}

#[tonic::async_trait]
impl QueryServiceTrait for QueryGrpcService {
    type FollowLogsStream =
        Pin<Box<dyn Stream<Item = Result<QueryLogsResponse, Status>> + Send + 'static>>;

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
        let filter = build_log_filter(&req);
        let limit = effective_limit(req.limit);
        let store = self.store.read().await;
        let resource_logs = store.query_logs(&filter, limit);
        Ok(Response::new(QueryLogsResponse { resource_logs }))
    }

    async fn follow_logs(
        &self,
        request: Request<QueryLogsRequest>,
    ) -> Result<Response<Self::FollowLogsStream>, Status> {
        let req = request.into_inner();
        let limit = effective_limit(req.limit);
        let store = self.store.clone();

        // Initial batch
        let filter = build_log_filter(&req);
        let initial_logs = {
            let s = store.read().await;
            s.query_logs(&filter, limit)
        };

        // Track the latest timestamp we've sent so we can send only newer logs
        let mut last_ts: u64 = initial_logs
            .iter()
            .map(|rl| crate::store::log_sort_key_pub(rl))
            .max()
            .unwrap_or(0);

        let event_rx = store.read().await.subscribe();
        let event_stream = BroadcastStream::new(event_rx);

        let stream = async_stream::try_stream! {
            // Send initial batch
            if !initial_logs.is_empty() {
                yield QueryLogsResponse { resource_logs: initial_logs };
            }

            // Wait for new log events
            tokio::pin!(event_stream);
            while let Some(event_result) = event_stream.next().await {
                let event = match event_result {
                    Ok(e) => e,
                    Err(_) => continue, // lagged, skip
                };
                if !matches!(event, StoreEvent::LogsAdded) {
                    continue;
                }

                // Query only logs newer than the last sent
                let mut delta_filter = build_log_filter(&req);
                delta_filter.start_time_ns = Some(last_ts + 1);

                let new_logs = {
                    let s = store.read().await;
                    s.query_logs(&delta_filter, usize::MAX)
                };

                if !new_logs.is_empty() {
                    if let Some(max_ts) = new_logs.iter().map(|rl| crate::store::log_sort_key_pub(rl)).max() {
                        last_ts = max_ts;
                    }
                    yield QueryLogsResponse { resource_logs: new_logs };
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
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

    async fn clear_traces(
        &self,
        _request: Request<ClearTracesRequest>,
    ) -> Result<Response<ClearResponse>, Status> {
        self.store.write().await.clear_traces();
        Ok(Response::new(ClearResponse {}))
    }

    async fn clear_logs(
        &self,
        _request: Request<ClearLogsRequest>,
    ) -> Result<Response<ClearResponse>, Status> {
        self.store.write().await.clear_logs();
        Ok(Response::new(ClearResponse {}))
    }

    async fn clear_metrics(
        &self,
        _request: Request<ClearMetricsRequest>,
    ) -> Result<Response<ClearResponse>, Status> {
        self.store.write().await.clear_metrics();
        Ok(Response::new(ClearResponse {}))
    }
}
