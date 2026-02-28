use crate::cli::OutputFormat;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::{Row as ProtoRow, SqlQueryRequest};
use crate::query::sql::convert::trace_flags_to_sql;

use super::{
    get_row_kvlist, get_row_string, parse_time_spec, print_kvlist, print_rows_csv, print_rows_jsonl,
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
        OutputFormat::Jsonl => print_rows_jsonl(&response.rows)?,
        OutputFormat::Csv => print_rows_csv(&response.rows, true),
        OutputFormat::Text => print_trace_rows_text(&response.rows),
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

    let mut csv_header_shown = false;
    while let Some(msg) = stream.message().await? {
        match format {
            OutputFormat::Jsonl => print_rows_jsonl(&msg.rows)?,
            OutputFormat::Csv => {
                print_rows_csv(&msg.rows, !csv_header_shown);
                csv_header_shown = true;
            }
            OutputFormat::Text => print_trace_rows_text(&msg.rows),
        }
    }

    Ok(())
}

pub fn print_trace_rows_text(rows: &[ProtoRow]) {
    for row in rows {
        // Header: Trace: {trace_id}
        if let Some(trace_id) = get_row_string(row, "trace_id") {
            println!("Trace: {}", trace_id);
        }

        // Span line
        let span_name = get_row_string(row, "span_name");
        let span_id = get_row_string(row, "span_id");
        match (span_name.as_deref(), span_id.as_deref()) {
            (Some(name), Some(id)) => println!("  Span: {} [{}]", name, id),
            (Some(name), None) => println!("  Span: {}", name),
            (None, Some(id)) => println!("  Span: [{}]", id),
            _ => {}
        }

        // Status
        if let Some(status) = get_row_string(row, "status_code") {
            println!("    Status: {}", status);
        }

        // Start time and duration
        let start_time = get_row_string(row, "start_time");
        let duration = get_row_string(row, "duration_ns");
        match (start_time.as_deref(), duration.as_deref()) {
            (Some(st), Some(dur)) => println!("    Start: {} Duration: {}ns", st, dur),
            (Some(st), None) => println!("    Start: {}", st),
            (None, Some(dur)) => println!("    Duration: {}ns", dur),
            _ => {}
        }

        // Resource
        if let Some(kvs) = get_row_kvlist(row, "resource") {
            print_kvlist(kvs, "Resource", "    ");
        }

        // Attributes
        if let Some(kvs) = get_row_kvlist(row, "attributes") {
            print_kvlist(kvs, "Attributes", "    ");
        }
    }
}
