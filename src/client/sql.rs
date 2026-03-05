use crate::cli::SqlOutputFormat;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::{Row as ProtoRow, SqlQueryRequest};

pub async fn query_sql(
    server: &str,
    query: &str,
    format: &SqlOutputFormat,
    show_trace_id: bool,
) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let response = client
        .sql_query(SqlQueryRequest {
            query: query.to_string(),
        })
        .await?;

    if show_trace_id {
        if let Some(trace_id) = response.metadata().get("x-trace-id") {
            eprintln!("trace_id: {}", trace_id.to_str().unwrap_or("?"));
        }
    }

    print_rows(&response.into_inner().rows, format, true)?;
    Ok(())
}

pub async fn follow_sql(
    server: &str,
    query: &str,
    format: &SqlOutputFormat,
    show_trace_id: bool,
) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let response = client
        .follow_sql(SqlQueryRequest {
            query: query.to_string(),
        })
        .await?;

    if show_trace_id {
        if let Some(trace_id) = response.metadata().get("x-trace-id") {
            eprintln!("trace_id: {}", trace_id.to_str().unwrap_or("?"));
        }
    }

    let mut stream = response.into_inner();
    let mut csv_header_shown = false;
    while let Some(msg) = stream.message().await? {
        print_rows(&msg.rows, format, !csv_header_shown)?;
        csv_header_shown = true;
    }
    Ok(())
}

fn print_rows(rows: &[ProtoRow], format: &SqlOutputFormat, csv_header: bool) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    match format {
        SqlOutputFormat::Jsonl => super::print_rows_jsonl(rows)?,
        SqlOutputFormat::Csv => super::print_rows_csv(rows, csv_header),
        SqlOutputFormat::Table => super::print_rows_table(rows),
    }
    Ok(())
}
