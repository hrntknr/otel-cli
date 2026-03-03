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
use crate::store::{SharedStore, Store, StoreEvent};

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
        Ok(Response::new(SqlQueryResponse { rows }))
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
                yield SqlQueryResponse { rows: initial_rows };
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
                            yield SqlQueryResponse { rows };
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
        let stream = build_follow_stream(
            self.store.clone(),
            |s| s.all_traces().iter().cloned().collect(),
            crate::store::rs_sort_key,
            StoreEvent::TracesAdded,
            Store::query_traces_since,
            |items| FollowTracesResponse {
                resource_spans: items,
            },
        )
        .await;
        Ok(Response::new(stream))
    }

    #[instrument(name = "query.follow_logs", skip_all)]
    async fn follow_logs(
        &self,
        _request: Request<FollowRequest>,
    ) -> Result<Response<Self::FollowLogsStream>, Status> {
        tracing::debug!("starting follow_logs stream");
        let stream = build_follow_stream(
            self.store.clone(),
            |s| s.all_logs().iter().cloned().collect(),
            crate::store::log_sort_key,
            StoreEvent::LogsAdded,
            Store::query_logs_since,
            |items| FollowLogsResponse {
                resource_logs: items,
            },
        )
        .await;
        Ok(Response::new(stream))
    }

    #[instrument(name = "query.follow_metrics", skip_all)]
    async fn follow_metrics(
        &self,
        _request: Request<FollowRequest>,
    ) -> Result<Response<Self::FollowMetricsStream>, Status> {
        tracing::debug!("starting follow_metrics stream");
        let stream = build_follow_stream(
            self.store.clone(),
            |s| s.all_metrics().iter().cloned().collect(),
            crate::store::metric_sort_key,
            StoreEvent::MetricsAdded,
            Store::query_metrics_since,
            |items| FollowMetricsResponse {
                resource_metrics: items,
            },
        )
        .await;
        Ok(Response::new(stream))
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

async fn build_follow_stream<T, R>(
    store: SharedStore,
    get_initial: fn(&Store) -> Vec<T>,
    sort_key_fn: fn(&T) -> u64,
    event_filter: StoreEvent,
    query_since_fn: fn(&Store, u64) -> Vec<T>,
    wrap_fn: fn(Vec<T>) -> R,
) -> Pin<Box<dyn Stream<Item = Result<R, Status>> + Send + 'static>>
where
    T: Send + 'static,
    R: Send + 'static,
{
    let initial = {
        let s = store.read().await;
        get_initial(&s)
    };
    let mut last_ts: u64 = initial.iter().map(sort_key_fn).max().unwrap_or(0);
    let event_rx = store.read().await.subscribe();
    let event_stream = BroadcastStream::new(event_rx);

    let stream = async_stream::try_stream! {
        if !initial.is_empty() {
            yield wrap_fn(initial);
        }

        tokio::pin!(event_stream);
        while let Some(event_result) = event_stream.next().await {
            if !matches!(&event_result, Ok(e) if *e == event_filter) {
                continue;
            }
            let s = store.read().await;
            let items = query_since_fn(&s, last_ts + 1);
            if items.is_empty() {
                continue;
            }
            if let Some(max_ts) = items.iter().map(sort_key_fn).max() {
                last_ts = max_ts;
            }
            yield wrap_fn(items);
        }
    };

    Box::pin(stream)
}
