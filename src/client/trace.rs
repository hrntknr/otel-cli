use crate::cli::OutputFormat;
use crate::proto::otelcli::query::v1::Row as ProtoRow;
use crate::query::sql::convert::trace_flags_to_sql;

use super::{get_row_kvlist, get_row_string, get_row_timestamp, parse_time_spec, print_kvlist};

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
    super::query_and_print(server, &sql, format, print_trace_rows_text).await
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
    super::follow_and_print(server, &sql, format, print_trace_rows_text).await
}

pub fn print_trace_rows_text(rows: &[ProtoRow]) {
    // Group spans by trace_id, preserving first-appearance order
    let mut groups: Vec<(String, Vec<&ProtoRow>)> = Vec::new();
    for row in rows {
        let tid = get_row_string(row, "trace_id").unwrap_or_default();
        if let Some(group) = groups.iter_mut().find(|(id, _)| id == &tid) {
            group.1.push(row);
        } else {
            groups.push((tid, vec![row]));
        }
    }

    for (i, (trace_id, spans)) in groups.iter().enumerate() {
        if i > 0 {
            println!();
        }
        println!("Trace: {}", trace_id);
        for row in spans {
            print_span_text(row);
        }
    }
}

fn print_span_text(row: &ProtoRow) {
    let span_name = get_row_string(row, "span_name");
    let span_id = get_row_string(row, "span_id");
    match (span_name.as_deref(), span_id.as_deref()) {
        (Some(name), Some(id)) => println!("  Span: {} [{}]", name, id),
        (Some(name), None) => println!("  Span: {}", name),
        (None, Some(id)) => println!("  Span: [{}]", id),
        _ => {}
    }

    if let Some(status) = get_row_string(row, "status_code") {
        println!("    Status: {}", status);
    }

    let start_time = get_row_timestamp(row, "start_time");
    let duration = get_row_string(row, "duration_ns");
    match (start_time.as_deref(), duration.as_deref()) {
        (Some(st), Some(dur)) => println!("    Start: {} Duration: {}ns", st, dur),
        (Some(st), None) => println!("    Start: {}", st),
        (None, Some(dur)) => println!("    Duration: {}ns", dur),
        _ => {}
    }

    if let Some(kvs) = get_row_kvlist(row, "resource") {
        print_kvlist(kvs, "Resource", "    ");
    }

    if let Some(kvs) = get_row_kvlist(row, "attributes") {
        print_kvlist(kvs, "Attributes", "    ");
    }
}
