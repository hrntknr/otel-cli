use std::pin::Pin;

use tokio_stream::{wrappers::BroadcastStream, Stream, StreamExt};
use tonic::{Request, Response, Status};

use crate::proto::otelcli::query::v1::{
    query_service_server::QueryService as QueryServiceTrait, ClearLogsRequest, ClearMetricsRequest,
    ClearResponse, ClearTracesRequest, SqlQueryRequest, SqlQueryResponse,
};
use crate::query::{QueryResult, TargetTable};
use crate::store::{SharedStore, StoreEvent};

pub struct QueryGrpcService {
    store: SharedStore,
}

impl QueryGrpcService {
    pub fn new(store: SharedStore) -> Self {
        Self { store }
    }
}

fn to_sql_response(result: QueryResult) -> SqlQueryResponse {
    match result {
        QueryResult::Traces(groups) => SqlQueryResponse {
            trace_groups: groups
                .into_iter()
                .map(|g| crate::proto::otelcli::query::v1::TraceGroup {
                    trace_id: g.trace_id,
                    resource_spans: g.resource_spans,
                })
                .collect(),
            resource_logs: vec![],
            resource_metrics: vec![],
        },
        QueryResult::Logs(logs) => SqlQueryResponse {
            trace_groups: vec![],
            resource_logs: logs,
            resource_metrics: vec![],
        },
        QueryResult::Metrics(metrics) => SqlQueryResponse {
            trace_groups: vec![],
            resource_logs: vec![],
            resource_metrics: metrics,
        },
    }
}

#[tonic::async_trait]
impl QueryServiceTrait for QueryGrpcService {
    type FollowSqlStream =
        Pin<Box<dyn Stream<Item = Result<SqlQueryResponse, Status>> + Send + 'static>>;

    async fn sql_query(
        &self,
        request: Request<SqlQueryRequest>,
    ) -> Result<Response<SqlQueryResponse>, Status> {
        let req = request.into_inner();
        let parsed = crate::query::sql::parse(&req.query)
            .map_err(|e| Status::invalid_argument(format!("SQL parse error: {}", e)))?;
        let store = self.store.read().await;
        let result = crate::query::sql::execute(&store, &parsed);
        Ok(Response::new(to_sql_response(result)))
    }

    async fn follow_sql(
        &self,
        request: Request<SqlQueryRequest>,
    ) -> Result<Response<Self::FollowSqlStream>, Status> {
        let req = request.into_inner();
        let parsed = crate::query::sql::parse(&req.query)
            .map_err(|e| Status::invalid_argument(format!("SQL parse error: {}", e)))?;

        let target = parsed.table.clone();
        let store = self.store.clone();

        // Initial query
        let initial_result = {
            let s = store.read().await;
            crate::query::sql::execute(&s, &parsed)
        };

        // Track latest timestamp for delta queries
        let mut last_ts: u64 = match &initial_result {
            QueryResult::Logs(logs) => logs
                .iter()
                .map(crate::store::log_sort_key)
                .max()
                .unwrap_or(0),
            QueryResult::Metrics(metrics) => metrics
                .iter()
                .map(crate::store::metric_sort_key)
                .max()
                .unwrap_or(0),
            QueryResult::Traces(_) => {
                let s = store.read().await;
                s.current_trace_version()
            }
        };

        let event_rx = store.read().await.subscribe();
        let event_stream = BroadcastStream::new(event_rx);

        let stream = async_stream::try_stream! {
            // Send initial batch
            let initial_response = to_sql_response(initial_result);
            if !initial_response.trace_groups.is_empty()
                || !initial_response.resource_logs.is_empty()
                || !initial_response.resource_metrics.is_empty()
            {
                yield initial_response;
            }

            // Wait for new events
            tokio::pin!(event_stream);
            while let Some(event_result) = event_stream.next().await {
                let event = match event_result {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                let matches_event = match (&target, &event) {
                    (TargetTable::Traces, StoreEvent::TracesAdded) => true,
                    (TargetTable::Logs, StoreEvent::LogsAdded) => true,
                    (TargetTable::Metrics, StoreEvent::MetricsAdded) => true,
                    _ => false,
                };
                if !matches_event {
                    continue;
                }

                let s = store.read().await;
                let response = match &target {
                    TargetTable::Traces => {
                        let groups = s.query_traces_since_version(last_ts);
                        last_ts = s.current_trace_version();
                        if groups.is_empty() {
                            continue;
                        }
                        SqlQueryResponse {
                            trace_groups: groups
                                .into_iter()
                                .map(|g| crate::proto::otelcli::query::v1::TraceGroup {
                                    trace_id: g.trace_id,
                                    resource_spans: g.resource_spans,
                                })
                                .collect(),
                            resource_logs: vec![],
                            resource_metrics: vec![],
                        }
                    }
                    TargetTable::Logs => {
                        let logs = s.query_logs_since(last_ts + 1);
                        if logs.is_empty() {
                            continue;
                        }
                        if let Some(max_ts) = logs.iter().map(crate::store::log_sort_key).max() {
                            last_ts = max_ts;
                        }
                        SqlQueryResponse {
                            trace_groups: vec![],
                            resource_logs: logs,
                            resource_metrics: vec![],
                        }
                    }
                    TargetTable::Metrics => {
                        let metrics = s.query_metrics_since(last_ts + 1);
                        if metrics.is_empty() {
                            continue;
                        }
                        if let Some(max_ts) =
                            metrics.iter().map(crate::store::metric_sort_key).max()
                        {
                            last_ts = max_ts;
                        }
                        SqlQueryResponse {
                            trace_groups: vec![],
                            resource_logs: vec![],
                            resource_metrics: metrics,
                        }
                    }
                };
                yield response;
            }
        };

        Ok(Response::new(Box::pin(stream)))
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
