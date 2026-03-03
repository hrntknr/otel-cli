use std::pin::Pin;

use datafusion::prelude::SessionContext;
use tokio_stream::{wrappers::BroadcastStream, Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::instrument;

use crate::proto::otelcli::query::v1::{
    query_service_server::QueryService as QueryServiceTrait, ClearLogsRequest, ClearMetricsRequest,
    ClearResponse, ClearTracesRequest, FollowLogsResponse, FollowMetricsResponse, FollowRequest,
    FollowTracesResponse, ShutdownRequest, ShutdownResponse, SqlQueryRequest, SqlQueryResponse,
    StatusRequest, StatusResponse,
};
use crate::store::{SharedStore, StoreEvent};

pub struct QueryGrpcService {
    store: SharedStore,
    ctx: SessionContext,
    shutdown: CancellationToken,
}

impl QueryGrpcService {
    pub fn new(store: SharedStore, ctx: SessionContext, shutdown: CancellationToken) -> Self {
        Self {
            store,
            ctx,
            shutdown,
        }
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

    #[instrument(name = "query.sql_query", skip_all, fields(db.statement))]
    async fn sql_query(
        &self,
        request: Request<SqlQueryRequest>,
    ) -> Result<Response<SqlQueryResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("db.statement", &req.query);
        tracing::debug!(query = %req.query, "executing SQL query");
        let rows = crate::query::sql::execute(&self.ctx, &req.query)
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, query = %req.query, "SQL query error");
                Status::invalid_argument(format!("SQL error: {}", e))
            })?;
        tracing::debug!(rows = rows.len(), "SQL query completed");
        Ok(Response::new(rows_to_proto(rows)))
    }

    #[instrument(name = "query.follow_sql", skip_all)]
    async fn follow_sql(
        &self,
        request: Request<SqlQueryRequest>,
    ) -> Result<Response<Self::FollowSqlStream>, Status> {
        let req = request.into_inner();
        tracing::debug!(query = %req.query, "starting SQL follow stream");
        let sql = req.query.clone();

        let ctx = self.ctx.clone();
        let store = self.store.clone();

        // Validate the SQL upfront
        let initial_rows = crate::query::sql::execute(&ctx, &sql).await.map_err(|e| {
            tracing::warn!(error = %e, query = %sql, "SQL query error");
            Status::invalid_argument(format!("SQL error: {}", e))
        })?;

        let event_rx = store.read().await.subscribe();
        let event_stream = BroadcastStream::new(event_rx);

        let stream = async_stream::try_stream! {
            // Send initial batch
            if !initial_rows.is_empty() {
                yield rows_to_proto(initial_rows);
            }

            // Wait for new events and re-execute the full query each time
            tokio::pin!(event_stream);
            while let Some(event_result) = event_stream.next().await {
                let event = match event_result {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                let is_data_event = matches!(
                    event,
                    StoreEvent::TracesAdded | StoreEvent::LogsAdded | StoreEvent::MetricsAdded
                );
                if !is_data_event {
                    continue;
                }

                match crate::query::sql::execute(&ctx, &sql).await {
                    Ok(rows) => {
                        if !rows.is_empty() {
                            yield rows_to_proto(rows);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "follow_sql re-execution error");
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    #[instrument(name = "query.follow_traces", skip_all)]
    async fn follow_traces(
        &self,
        _request: Request<FollowRequest>,
    ) -> Result<Response<Self::FollowTracesStream>, Status> {
        tracing::debug!("starting follow_traces stream");
        let store = self.store.clone();

        let initial_traces = {
            let s = store.read().await;
            s.all_traces().iter().cloned().collect::<Vec<_>>()
        };
        let mut last_ts: u64 = initial_traces
            .iter()
            .map(crate::store::rs_sort_key)
            .max()
            .unwrap_or(0);

        let event_rx = store.read().await.subscribe();
        let event_stream = BroadcastStream::new(event_rx);

        let stream = async_stream::try_stream! {
            if !initial_traces.is_empty() {
                yield FollowTracesResponse { resource_spans: initial_traces };
            }

            tokio::pin!(event_stream);
            while let Some(event_result) = event_stream.next().await {
                if !matches!(event_result, Ok(StoreEvent::TracesAdded)) {
                    continue;
                }
                let s = store.read().await;
                let traces = s.query_traces_since(last_ts + 1);
                if traces.is_empty() {
                    continue;
                }
                if let Some(max_ts) = traces.iter().map(crate::store::rs_sort_key).max() {
                    last_ts = max_ts;
                }
                yield FollowTracesResponse { resource_spans: traces };
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    #[instrument(name = "query.follow_logs", skip_all)]
    async fn follow_logs(
        &self,
        _request: Request<FollowRequest>,
    ) -> Result<Response<Self::FollowLogsStream>, Status> {
        tracing::debug!("starting follow_logs stream");
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

    #[instrument(name = "query.follow_metrics", skip_all)]
    async fn follow_metrics(
        &self,
        _request: Request<FollowRequest>,
    ) -> Result<Response<Self::FollowMetricsStream>, Status> {
        tracing::debug!("starting follow_metrics stream");
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

    #[instrument(name = "query.clear_traces", skip_all)]
    async fn clear_traces(
        &self,
        _request: Request<ClearTracesRequest>,
    ) -> Result<Response<ClearResponse>, Status> {
        tracing::debug!("clearing traces");
        self.store.write().await.clear_traces();
        Ok(Response::new(ClearResponse {}))
    }

    #[instrument(name = "query.clear_logs", skip_all)]
    async fn clear_logs(
        &self,
        _request: Request<ClearLogsRequest>,
    ) -> Result<Response<ClearResponse>, Status> {
        tracing::debug!("clearing logs");
        self.store.write().await.clear_logs();
        Ok(Response::new(ClearResponse {}))
    }

    #[instrument(name = "query.clear_metrics", skip_all)]
    async fn clear_metrics(
        &self,
        _request: Request<ClearMetricsRequest>,
    ) -> Result<Response<ClearResponse>, Status> {
        tracing::debug!("clearing metrics");
        self.store.write().await.clear_metrics();
        Ok(Response::new(ClearResponse {}))
    }

    #[instrument(name = "query.status", skip_all)]
    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        tracing::debug!("status request");
        let store = self.store.read().await;
        Ok(Response::new(StatusResponse {
            trace_count: store.trace_count() as u64,
            log_count: store.log_count() as u64,
            metric_count: store.metric_count() as u64,
        }))
    }

    #[instrument(name = "query.shutdown", skip_all)]
    async fn shutdown(
        &self,
        _request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        tracing::info!("shutdown requested via RPC");
        self.shutdown.cancel();
        Ok(Response::new(ShutdownResponse {}))
    }
}
