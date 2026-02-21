use crate::cli::OutputFormat;
use crate::proto::opentelemetry::proto::trace::v1::ResourceSpans;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::QueryTracesRequest;

use super::{
    extract_any_value_string, format_attributes_json, format_timestamp, get_resource_attributes,
    hex_encode,
};

pub async fn query_traces(
    server: &str,
    service: Option<String>,
    trace_id: Option<String>,
    attributes: Vec<(String, String)>,
    limit: i32,
    format: &OutputFormat,
) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;

    let request = QueryTracesRequest {
        service_name: service.unwrap_or_default(),
        trace_id: trace_id.unwrap_or_default(),
        attributes: attributes.into_iter().collect(),
        limit,
    };

    let response = client.query_traces(request).await?.into_inner();

    match format {
        OutputFormat::Json => {
            print_traces_json(&response.resource_spans)?;
        }
        OutputFormat::Text => {
            print_traces_text(&response.resource_spans);
        }
        OutputFormat::Toon => {
            print_traces_toon(&response.resource_spans)?;
        }
    }

    Ok(())
}

fn print_traces_text(resource_spans: &[ResourceSpans]) {
    for rs in resource_spans {
        let resource_attrs = get_resource_attributes(&rs.resource);
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                let trace_id = hex_encode(&span.trace_id);
                let span_id = hex_encode(&span.span_id);
                let status_code = span.status.as_ref().map(|s| s.code).unwrap_or(0);
                let duration = span
                    .end_time_unix_nano
                    .saturating_sub(span.start_time_unix_nano);

                println!("Trace: {}", trace_id);
                println!("  Span: {} [{}]", span.name, span_id);
                println!("    Status: {}", status_code);
                println!(
                    "    Start: {} Duration: {}ns",
                    format_timestamp(span.start_time_unix_nano),
                    duration
                );
                if !resource_attrs.is_empty() {
                    println!("    Resource:");
                    for kv in resource_attrs {
                        let val = kv
                            .value
                            .as_ref()
                            .map(extract_any_value_string)
                            .unwrap_or_default();
                        println!("      {}: {}", kv.key, val);
                    }
                }
                if !span.attributes.is_empty() {
                    println!("    Attributes:");
                    for kv in &span.attributes {
                        let val = kv
                            .value
                            .as_ref()
                            .map(extract_any_value_string)
                            .unwrap_or_default();
                        println!("      {}: {}", kv.key, val);
                    }
                }
            }
        }
    }
}

fn build_traces_value(resource_spans: &[ResourceSpans]) -> Vec<serde_json::Value> {
    let mut traces = Vec::new();

    for rs in resource_spans {
        let resource_attrs = get_resource_attributes(&rs.resource);
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                let trace_id = hex_encode(&span.trace_id);
                let span_id = hex_encode(&span.span_id);
                let status_code = span.status.as_ref().map(|s| s.code).unwrap_or(0);

                let entry = serde_json::json!({
                    "trace_id": trace_id,
                    "span_id": span_id,
                    "resource_attributes": format_attributes_json(resource_attrs),
                    "name": span.name,
                    "status": status_code,
                    "start_time": format_timestamp(span.start_time_unix_nano),
                    "end_time": format_timestamp(span.end_time_unix_nano),
                    "attributes": format_attributes_json(&span.attributes),
                });
                traces.push(entry);
            }
        }
    }

    traces
}

fn print_traces_json(resource_spans: &[ResourceSpans]) -> anyhow::Result<()> {
    let traces = build_traces_value(resource_spans);
    println!("{}", serde_json::to_string_pretty(&traces)?);
    Ok(())
}

fn print_traces_toon(resource_spans: &[ResourceSpans]) -> anyhow::Result<()> {
    let traces = build_traces_value(resource_spans);
    println!(
        "{}",
        toon_format::encode_default(&serde_json::json!(traces))?
    );
    Ok(())
}
