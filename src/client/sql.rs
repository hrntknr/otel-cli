use crate::cli::OutputFormat;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::{Row as ProtoRow, SqlQueryRequest};
use crate::query::sql::parser;

pub async fn query_sql(server: &str, query: &str, format: &OutputFormat) -> anyhow::Result<()> {
    let table = parse_table_type(query)?;

    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let response = client
        .sql_query(SqlQueryRequest {
            query: query.to_string(),
        })
        .await?
        .into_inner();

    print_rows(&response.rows, &table, format, true)?;
    Ok(())
}

pub async fn follow_sql(server: &str, query: &str, format: &OutputFormat) -> anyhow::Result<()> {
    let table = parse_table_type(query)?;

    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let mut stream = client
        .follow_sql(SqlQueryRequest {
            query: query.to_string(),
        })
        .await?
        .into_inner();

    let mut csv_header_shown = false;
    while let Some(msg) = stream.message().await? {
        print_rows(&msg.rows, &table, format, !csv_header_shown)?;
        csv_header_shown = true;
    }
    Ok(())
}

fn parse_table_type(query: &str) -> anyhow::Result<crate::query::TargetTable> {
    let parsed = parser::parse(query).map_err(|e| anyhow::anyhow!("SQL parse error: {}", e))?;
    Ok(parsed.table)
}

fn print_rows(
    rows: &[ProtoRow],
    table: &crate::query::TargetTable,
    format: &OutputFormat,
    csv_header: bool,
) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    match format {
        OutputFormat::Jsonl => super::print_rows_jsonl(rows)?,
        OutputFormat::Csv => super::print_rows_csv(rows, csv_header),
        OutputFormat::Text => match table {
            crate::query::TargetTable::Traces => {
                super::trace::print_trace_rows_text(rows);
            }
            crate::query::TargetTable::Logs => {
                super::log::print_log_rows_text(rows);
            }
            crate::query::TargetTable::Metrics => {
                super::metrics::print_metric_rows_text(rows);
            }
        },
    }
    Ok(())
}
