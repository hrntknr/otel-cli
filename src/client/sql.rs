use crate::cli::SqlOutputFormat;
use crate::proto::otelcli::query::v1::query_service_client::QueryServiceClient;
use crate::proto::otelcli::query::v1::{Row as ProtoRow, SqlQueryRequest};

pub async fn query_sql(server: &str, query: &str, format: &SqlOutputFormat) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let response = client
        .sql_query(SqlQueryRequest {
            query: query.to_string(),
        })
        .await?
        .into_inner();

    print_rows(&response.rows, format, true)?;
    Ok(())
}

pub async fn follow_sql(
    server: &str,
    query: &str,
    format: &SqlOutputFormat,
) -> anyhow::Result<()> {
    let mut client = QueryServiceClient::connect(server.to_string()).await?;
    let mut stream = client
        .follow_sql(SqlQueryRequest {
            query: query.to_string(),
        })
        .await?
        .into_inner();

    let mut csv_header_shown = false;
    while let Some(msg) = stream.message().await? {
        print_rows(&msg.rows, format, !csv_header_shown)?;
        csv_header_shown = true;
    }
    Ok(())
}

fn print_rows(
    rows: &[ProtoRow],
    format: &SqlOutputFormat,
    csv_header: bool,
) -> anyhow::Result<()> {
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
