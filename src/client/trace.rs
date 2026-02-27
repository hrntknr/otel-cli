use crate::cli::OutputFormat;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::{SqlQueryRequest, TraceGroup};
use crate::query::sql::convert::trace_flags_to_sql;

use super::{
    extract_any_value_string, format_attributes_json, format_timestamp, get_resource_attributes,
    hex_encode, parse_time_spec,
};

#[allow(clippy::too_many_arguments)]
pub async fn query_traces(
    server: &str,
    service: Option<String>,
    trace_id: Option<String>,
    attributes: Vec<(String, String)>,
    limit: i32,
    format: &OutputFormat,
    since: Option<String>,
    until: Option<String>,
) -> anyhow::Result<()> {
    let start_time_ns = since.as_deref().map(parse_time_spec).transpose()?;
    let end_time_ns = until.as_deref().map(parse_time_spec).transpose()?;
    let sql = trace_flags_to_sql(
        service.as_deref(),
        trace_id.as_deref(),
        &attributes,
        Some(limit as usize),
        start_time_ns,
        end_time_ns,
    );

    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let response = client
        .sql_query(SqlQueryRequest { query: sql })
        .await?
        .into_inner();

    match format {
        OutputFormat::Json => {
            print_traces_json(&response.trace_groups)?;
        }
        OutputFormat::Text => {
            print_traces_text(&response.trace_groups);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn follow_traces(
    server: &str,
    service: Option<String>,
    trace_id: Option<String>,
    attributes: Vec<(String, String)>,
    limit: i32,
    format: &OutputFormat,
    since: Option<String>,
    until: Option<String>,
    _delta: bool,
) -> anyhow::Result<()> {
    let start_time_ns = since.as_deref().map(parse_time_spec).transpose()?;
    let end_time_ns = until.as_deref().map(parse_time_spec).transpose()?;
    let sql = trace_flags_to_sql(
        service.as_deref(),
        trace_id.as_deref(),
        &attributes,
        Some(limit as usize),
        start_time_ns,
        end_time_ns,
    );

    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let mut stream = client
        .follow_sql(SqlQueryRequest { query: sql })
        .await?
        .into_inner();

    while let Some(msg) = stream.message().await? {
        match format {
            OutputFormat::Json => {
                print_traces_json(&msg.trace_groups)?;
            }
            OutputFormat::Text => {
                print_traces_text(&msg.trace_groups);
            }
        }
    }

    Ok(())
}

pub fn print_traces_text(trace_groups: &[TraceGroup]) {
    for group in trace_groups {
        let trace_id = hex_encode(&group.trace_id);
        println!("Trace: {}", trace_id);
        for rs in &group.resource_spans {
            let resource_attrs = get_resource_attributes(&rs.resource);
            for ss in &rs.scope_spans {
                for span in &ss.spans {
                    let span_id = hex_encode(&span.span_id);
                    let status_code = span.status.as_ref().map(|s| s.code).unwrap_or(0);
                    let duration = span
                        .end_time_unix_nano
                        .saturating_sub(span.start_time_unix_nano);

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
}

fn build_traces_value(trace_groups: &[TraceGroup]) -> Vec<serde_json::Value> {
    let mut traces = Vec::new();

    for group in trace_groups {
        let trace_id = hex_encode(&group.trace_id);
        for rs in &group.resource_spans {
            let resource_attrs = get_resource_attributes(&rs.resource);
            for ss in &rs.scope_spans {
                for span in &ss.spans {
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
    }

    traces
}

pub fn print_traces_json(trace_groups: &[TraceGroup]) -> anyhow::Result<()> {
    let traces = build_traces_value(trace_groups);
    println!("{}", serde_json::to_string_pretty(&traces)?);
    Ok(())
}
