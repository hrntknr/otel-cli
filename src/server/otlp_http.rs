use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use base64::Engine;
use prost::Message;
use serde::de::DeserializeOwned;

use crate::proto::opentelemetry::proto::collector::{
    logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
    metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse},
    trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
};
use crate::store::SharedStore;

pub fn router(store: SharedStore) -> Router {
    Router::new()
        .route("/v1/traces", post(handle_traces))
        .route("/v1/logs", post(handle_logs))
        .route("/v1/metrics", post(handle_metrics))
        .with_state(store)
}

async fn handle_traces(
    State(store): State<SharedStore>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    let is_json = is_json_content_type(&headers);
    let request: ExportTraceServiceRequest = if is_json {
        decode_json(&body)?
    } else {
        ExportTraceServiceRequest::decode(body).map_err(|_| StatusCode::BAD_REQUEST)?
    };
    let mut s = store.write().await;
    s.insert_traces(request.resource_spans);
    let response = ExportTraceServiceResponse {
        partial_success: None,
    };
    Ok(encode_response(&response, is_json)?)
}

async fn handle_logs(
    State(store): State<SharedStore>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    let is_json = is_json_content_type(&headers);
    let request: ExportLogsServiceRequest = if is_json {
        decode_json(&body)?
    } else {
        ExportLogsServiceRequest::decode(body).map_err(|_| StatusCode::BAD_REQUEST)?
    };
    let mut s = store.write().await;
    s.insert_logs(request.resource_logs);
    let response = ExportLogsServiceResponse {
        partial_success: None,
    };
    Ok(encode_response(&response, is_json)?)
}

async fn handle_metrics(
    State(store): State<SharedStore>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    let is_json = is_json_content_type(&headers);
    let request: ExportMetricsServiceRequest = if is_json {
        decode_json(&body)?
    } else {
        ExportMetricsServiceRequest::decode(body).map_err(|_| StatusCode::BAD_REQUEST)?
    };
    let mut s = store.write().await;
    s.insert_metrics(request.resource_metrics);
    let response = ExportMetricsServiceResponse {
        partial_success: None,
    };
    Ok(encode_response(&response, is_json)?)
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.starts_with("application/json"))
        .unwrap_or(false)
}

/// Decode a JSON body, converting OTLP hex-encoded trace_id/span_id fields to base64.
fn decode_json<T: DeserializeOwned>(body: &[u8]) -> Result<T, StatusCode> {
    let mut value: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)?;
    convert_hex_ids_to_base64(&mut value);
    serde_json::from_value(value).map_err(|_| StatusCode::BAD_REQUEST)
}

fn encode_response<T: serde::Serialize + Message>(
    response: &T,
    is_json: bool,
) -> Result<impl IntoResponse, StatusCode> {
    if is_json {
        let body = serde_json::to_vec(response).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        Ok(([(header::CONTENT_TYPE, "application/json")], body))
    } else {
        Ok((
            [(header::CONTENT_TYPE, "application/x-protobuf")],
            response.encode_to_vec(),
        ))
    }
}

/// OTLP JSON uses hex encoding for traceId, spanId, and parentSpanId,
/// but pbjson expects base64 for bytes fields. This function recursively
/// converts these specific fields from hex to base64.
fn convert_hex_ids_to_base64(value: &mut serde_json::Value) {
    const HEX_ID_FIELDS: &[&str] = &["traceId", "spanId", "parentSpanId"];

    match value {
        serde_json::Value::Object(map) => {
            for (key, val) in map.iter_mut() {
                if HEX_ID_FIELDS.contains(&key.as_str()) {
                    if let serde_json::Value::String(s) = val {
                        if let Ok(bytes) = hex::decode(s.as_str()) {
                            *s = base64::engine::general_purpose::STANDARD.encode(&bytes);
                        }
                    }
                } else {
                    convert_hex_ids_to_base64(val);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr {
                convert_hex_ids_to_base64(item);
            }
        }
        _ => {}
    }
}
