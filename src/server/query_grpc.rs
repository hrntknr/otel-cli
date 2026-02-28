use std::pin::Pin;

use tokio_stream::{wrappers::BroadcastStream, Stream, StreamExt};
use tonic::{Request, Response, Status};

use crate::proto::otelcli::query::v1::{
    query_service_server::QueryService as QueryServiceTrait, ClearLogsRequest, ClearMetricsRequest,
    ClearResponse, ClearTracesRequest, FollowLogsResponse, FollowMetricsResponse, FollowRequest,
    FollowTracesResponse, SqlQueryRequest, SqlQueryResponse,
};
use crate::query::TargetTable;
use crate::store::{SharedStore, StoreEvent};

pub struct QueryGrpcService {
    store: SharedStore,
}

impl QueryGrpcService {
    pub fn new(store: SharedStore) -> Self {
        Self { store }
    }
}

fn rows_to_proto(rows: Vec<crate::query::Row>) -> SqlQueryResponse {
    use crate::proto::opentelemetry::proto::common::v1::{any_value, AnyValue, KeyValueList};

    fn row_value_to_any_value(rv: &crate::query::RowValue) -> Option<AnyValue> {
        match rv {
            crate::query::RowValue::String(s) => Some(AnyValue {
                value: Some(any_value::Value::StringValue(s.clone())),
            }),
            crate::query::RowValue::Int(i) => Some(AnyValue {
                value: Some(any_value::Value::IntValue(*i)),
            }),
            crate::query::RowValue::Double(d) => Some(AnyValue {
                value: Some(any_value::Value::DoubleValue(*d)),
            }),
            crate::query::RowValue::KeyValueList(kvs) => Some(AnyValue {
                value: Some(any_value::Value::KvlistValue(KeyValueList {
                    values: kvs.clone(),
                })),
            }),
            crate::query::RowValue::Null => None,
        }
    }

    SqlQueryResponse {
        rows: rows
            .into_iter()
            .map(|row| crate::proto::otelcli::query::v1::Row {
                columns: row
                    .into_iter()
                    .map(
                        |(name, value)| crate::proto::otelcli::query::v1::ColumnValue {
                            name,
                            value: row_value_to_any_value(&value),
                        },
                    )
                    .collect(),
            })
            .collect(),
    }
}

#[tonic::async_trait]
impl QueryServiceTrait for QueryGrpcService {
    type FollowSqlStream =
        Pin<Box<dyn Stream<Item = Result<SqlQueryResponse, Status>> + Send + 'static>>;
    type FollowTracesStream =
        Pin<Box<dyn Stream<Item = Result<FollowTracesResponse, Status>> + Send + 'static>>;
    type FollowLogsStream =
        Pin<Box<dyn Stream<Item = Result<FollowLogsResponse, Status>> + Send + 'static>>;
    type FollowMetricsStream =
        Pin<Box<dyn Stream<Item = Result<FollowMetricsResponse, Status>> + Send + 'static>>;

    async fn sql_query(
        &self,
        request: Request<SqlQueryRequest>,
    ) -> Result<Response<SqlQueryResponse>, Status> {
        let req = request.into_inner();
        let parsed = crate::query::sql::parse(&req.query)
            .map_err(|e| Status::invalid_argument(format!("SQL parse error: {}", e)))?;
        let store = self.store.read().await;
        let rows = crate::query::sql::execute(&store, &parsed);
        Ok(Response::new(rows_to_proto(rows)))
    }

    async fn follow_sql(
        &self,
        request: Request<SqlQueryRequest>,
    ) -> Result<Response<Self::FollowSqlStream>, Status> {
        let req = request.into_inner();
        let parsed = crate::query::sql::parse(&req.query)
            .map_err(|e| Status::invalid_argument(format!("SQL parse error: {}", e)))?;

        let target = parsed.table.clone();
        let projection = parsed.projection.clone();
        let store = self.store.clone();

        // Initial query: eval → track last_ts → project
        let initial_result = {
            let s = store.read().await;
            crate::query::sql::eval(&s, &parsed)
        };

        let mut last_ts: u64 = match &initial_result {
            crate::query::QueryResult::Logs(logs) => logs
                .iter()
                .map(crate::store::log_sort_key)
                .max()
                .unwrap_or(0),
            crate::query::QueryResult::Metrics(metrics) => metrics
                .iter()
                .map(crate::store::metric_sort_key)
                .max()
                .unwrap_or(0),
            crate::query::QueryResult::Traces(_) => {
                let s = store.read().await;
                s.current_trace_version()
            }
        };

        let initial_rows = crate::query::sql::project(&initial_result, &projection);

        let event_rx = store.read().await.subscribe();
        let event_stream = BroadcastStream::new(event_rx);

        let stream = async_stream::try_stream! {
            // Send initial batch
            if !initial_rows.is_empty() {
                yield rows_to_proto(initial_rows);
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
                let rows = match &target {
                    TargetTable::Traces => {
                        let groups = s.query_traces_since_version(last_ts);
                        last_ts = s.current_trace_version();
                        if groups.is_empty() {
                            continue;
                        }
                        crate::query::sql::project_traces(&groups, &projection)
                    }
                    TargetTable::Logs => {
                        let logs = s.query_logs_since(last_ts + 1);
                        if logs.is_empty() {
                            continue;
                        }
                        if let Some(max_ts) = logs.iter().map(crate::store::log_sort_key).max() {
                            last_ts = max_ts;
                        }
                        crate::query::sql::project_logs(&logs, &projection)
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
                        crate::query::sql::project_metrics(&metrics, &projection)
                    }
                };
                yield rows_to_proto(rows);
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn follow_traces(
        &self,
        _request: Request<FollowRequest>,
    ) -> Result<Response<Self::FollowTracesStream>, Status> {
        let store = self.store.clone();

        let initial_groups = {
            let s = store.read().await;
            s.all_traces().iter().cloned().collect::<Vec<_>>()
        };
        let mut last_version = store.read().await.current_trace_version();

        let event_rx = store.read().await.subscribe();
        let event_stream = BroadcastStream::new(event_rx);

        let stream = async_stream::try_stream! {
            if !initial_groups.is_empty() {
                let resource_spans = initial_groups
                    .into_iter()
                    .flat_map(|g| g.resource_spans)
                    .collect();
                yield FollowTracesResponse { resource_spans };
            }

            tokio::pin!(event_stream);
            while let Some(event_result) = event_stream.next().await {
                if !matches!(event_result, Ok(StoreEvent::TracesAdded)) {
                    continue;
                }
                let s = store.read().await;
                let groups = s.query_traces_since_version(last_version);
                last_version = s.current_trace_version();
                if groups.is_empty() {
                    continue;
                }
                let resource_spans = groups
                    .into_iter()
                    .flat_map(|g| g.resource_spans)
                    .collect();
                yield FollowTracesResponse { resource_spans };
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn follow_logs(
        &self,
        _request: Request<FollowRequest>,
    ) -> Result<Response<Self::FollowLogsStream>, Status> {
        let store = self.store.clone();

        let initial_logs = {
            let s = store.read().await;
            s.all_logs().iter().cloned().collect::<Vec<_>>()
        };
        let mut last_ts: u64 = initial_logs
            .iter()
            .map(crate::store::log_sort_key)
            .max()
            .unwrap_or(0);

        let event_rx = store.read().await.subscribe();
        let event_stream = BroadcastStream::new(event_rx);

        let stream = async_stream::try_stream! {
            if !initial_logs.is_empty() {
                yield FollowLogsResponse { resource_logs: initial_logs };
            }

            tokio::pin!(event_stream);
            while let Some(event_result) = event_stream.next().await {
                if !matches!(event_result, Ok(StoreEvent::LogsAdded)) {
                    continue;
                }
                let s = store.read().await;
                let logs = s.query_logs_since(last_ts + 1);
                if logs.is_empty() {
                    continue;
                }
                if let Some(max_ts) = logs.iter().map(crate::store::log_sort_key).max() {
                    last_ts = max_ts;
                }
                yield FollowLogsResponse { resource_logs: logs };
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn follow_metrics(
        &self,
        _request: Request<FollowRequest>,
    ) -> Result<Response<Self::FollowMetricsStream>, Status> {
        let store = self.store.clone();

        let initial_metrics = {
            let s = store.read().await;
            s.all_metrics().iter().cloned().collect::<Vec<_>>()
        };
        let mut last_ts: u64 = initial_metrics
            .iter()
            .map(crate::store::metric_sort_key)
            .max()
            .unwrap_or(0);

        let event_rx = store.read().await.subscribe();
        let event_stream = BroadcastStream::new(event_rx);

        let stream = async_stream::try_stream! {
            if !initial_metrics.is_empty() {
                yield FollowMetricsResponse { resource_metrics: initial_metrics };
            }

            tokio::pin!(event_stream);
            while let Some(event_result) = event_stream.next().await {
                if !matches!(event_result, Ok(StoreEvent::MetricsAdded)) {
                    continue;
                }
                let s = store.read().await;
                let metrics = s.query_metrics_since(last_ts + 1);
                if metrics.is_empty() {
                    continue;
                }
                if let Some(max_ts) = metrics.iter().map(crate::store::metric_sort_key).max() {
                    last_ts = max_ts;
                }
                yield FollowMetricsResponse { resource_metrics: metrics };
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
