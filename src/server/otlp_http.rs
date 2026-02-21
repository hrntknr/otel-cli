use axum::{
    body::Bytes,
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use prost::Message;

use crate::proto::opentelemetry::proto::collector::{
    logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
    metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse},
    trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
};
use crate::store::SharedStore;

/// A response type that serializes as protobuf with the appropriate content type.
struct ProtobufResponse(Vec<u8>);

impl IntoResponse for ProtobufResponse {
    fn into_response(self) -> axum::response::Response {
        ([(header::CONTENT_TYPE, "application/x-protobuf")], self.0).into_response()
    }
}

pub fn router(store: SharedStore) -> Router {
    Router::new()
        .route("/v1/traces", post(handle_traces))
        .route("/v1/logs", post(handle_logs))
        .route("/v1/metrics", post(handle_metrics))
        .with_state(store)
}

async fn handle_traces(
    State(store): State<SharedStore>,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    let request = ExportTraceServiceRequest::decode(body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut s = store.write().await;
    s.insert_traces(request.resource_spans);
    let response = ExportTraceServiceResponse {
        partial_success: None,
    };
    Ok(ProtobufResponse(response.encode_to_vec()))
}

async fn handle_logs(
    State(store): State<SharedStore>,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    let request = ExportLogsServiceRequest::decode(body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut s = store.write().await;
    s.insert_logs(request.resource_logs);
    let response = ExportLogsServiceResponse {
        partial_success: None,
    };
    Ok(ProtobufResponse(response.encode_to_vec()))
}

async fn handle_metrics(
    State(store): State<SharedStore>,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    let request = ExportMetricsServiceRequest::decode(body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut s = store.write().await;
    s.insert_metrics(request.resource_metrics);
    let response = ExportMetricsServiceResponse {
        partial_success: None,
    };
    Ok(ProtobufResponse(response.encode_to_vec()))
}
