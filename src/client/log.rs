use crate::cli::OutputFormat;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::{Row as ProtoRow, SqlQueryRequest};
use crate::query::sql::convert::log_flags_to_sql;

use super::{
    get_row_kvlist, get_row_string, parse_time_spec, print_kvlist, print_rows_csv, print_rows_jsonl,
};

#[allow(clippy::too_many_arguments)]
pub async fn query_logs(
    server: &str,
    service: Option<String>,
    severity: Option<String>,
    attributes: Vec<(String, String)>,
    limit: i32,
    format: &OutputFormat,
    since: Option<String>,
    until: Option<String>,
) -> anyhow::Result<()> {
    let start_time_ns = since.as_deref().map(parse_time_spec).transpose()?;
    let end_time_ns = until.as_deref().map(parse_time_spec).transpose()?;
    let sql = log_flags_to_sql(
        service.as_deref(),
        severity.as_deref(),
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
        OutputFormat::Text => print_log_rows_text(&response.rows),
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn follow_logs(
    server: &str,
    service: Option<String>,
    severity: Option<String>,
    attributes: Vec<(String, String)>,
    limit: i32,
    format: &OutputFormat,
    since: Option<String>,
    until: Option<String>,
) -> anyhow::Result<()> {
    let start_time_ns = since.as_deref().map(parse_time_spec).transpose()?;
    let end_time_ns = until.as_deref().map(parse_time_spec).transpose()?;
    let sql = log_flags_to_sql(
        service.as_deref(),
        severity.as_deref(),
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
            OutputFormat::Text => print_log_rows_text(&msg.rows),
        }
    }

    Ok(())
}

pub fn print_log_rows_text(rows: &[ProtoRow]) {
    for row in rows {
        // Header line: {timestamp} [{severity}] {body}
        let timestamp = get_row_string(row, "timestamp").unwrap_or_default();
        let severity = get_row_string(row, "severity").unwrap_or_default();
        let body = get_row_string(row, "body").unwrap_or_default();

        let has_timestamp = !timestamp.is_empty();
        let has_severity = !severity.is_empty();
        let has_body = !body.is_empty();

        match (has_timestamp, has_severity, has_body) {
            (true, true, true) => println!("{} [{}] {}", timestamp, severity, body),
            (true, true, false) => println!("{} [{}]", timestamp, severity),
            (true, false, true) => println!("{} {}", timestamp, body),
            (true, false, false) => println!("{}", timestamp),
            (false, true, true) => println!("[{}] {}", severity, body),
            (false, true, false) => println!("[{}]", severity),
            (false, false, true) => println!("{}", body),
            (false, false, false) => {}
        }

        // Resource
        if let Some(kvs) = get_row_kvlist(row, "resource") {
            print_kvlist(kvs, "Resource", "  ");
        }

        // Attributes
        if let Some(kvs) = get_row_kvlist(row, "attributes") {
            print_kvlist(kvs, "Attributes", "  ");
        }
    }
}
