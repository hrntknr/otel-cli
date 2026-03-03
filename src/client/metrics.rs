use crate::cli::OutputFormat;
use crate::proto::otelcli::query::v1::Row as ProtoRow;
use crate::query::sql::convert::metric_flags_to_sql;

use super::{get_row_kvlist, get_row_string, get_row_timestamp, parse_time_spec, print_kvlist};

pub async fn query_metrics(
    server: &str,
    service: Option<String>,
    name: Option<String>,
    limit: i32,
    format: &OutputFormat,
    since: Option<String>,
    until: Option<String>,
) -> anyhow::Result<()> {
    let start_time_ns = since.as_deref().map(parse_time_spec).transpose()?;
    let end_time_ns = until.as_deref().map(parse_time_spec).transpose()?;
    let sql = metric_flags_to_sql(
        service.as_deref(),
        name.as_deref(),
        Some(limit as usize),
        start_time_ns,
        end_time_ns,
    );
    super::query_and_print(server, &sql, format, print_metric_rows_text).await
}

pub async fn follow_metrics(
    server: &str,
    service: Option<String>,
    name: Option<String>,
    limit: i32,
    format: &OutputFormat,
    since: Option<String>,
    until: Option<String>,
) -> anyhow::Result<()> {
    let start_time_ns = since.as_deref().map(parse_time_spec).transpose()?;
    let end_time_ns = until.as_deref().map(parse_time_spec).transpose()?;
    let sql = metric_flags_to_sql(
        service.as_deref(),
        name.as_deref(),
        Some(limit as usize),
        start_time_ns,
        end_time_ns,
    );
    super::follow_and_print(server, &sql, format, print_metric_rows_text).await
}

pub fn print_metric_rows_text(rows: &[ProtoRow]) {
    for row in rows {
        // Header: Metric: {metric_name} ({type})
        let name = get_row_string(row, "metric_name");
        let mtype = get_row_string(row, "type");
        match (name.as_deref(), mtype.as_deref()) {
            (Some(n), Some(t)) => println!("Metric: {} ({})", n, t),
            (Some(n), None) => println!("Metric: {}", n),
            (None, Some(t)) => println!("Metric: ({})", t),
            _ => {}
        }

        // Resource
        if let Some(kvs) = get_row_kvlist(row, "resource") {
            print_kvlist(kvs, "Resource", "  ");
        }

        // Data point values
        let value = get_row_string(row, "value");
        let count = get_row_string(row, "count");
        let sum = get_row_string(row, "sum");
        let timestamp = get_row_timestamp(row, "timestamp");

        let has_value = value.is_some();
        let has_count = count.is_some();
        let has_sum = sum.is_some();

        if has_value || has_count || has_sum {
            println!("  Data points:");
            if has_value {
                match timestamp.as_deref() {
                    Some(ts) => println!("    Value: {} Time: {}", value.unwrap(), ts),
                    None => println!("    Value: {}", value.unwrap()),
                }
            } else if has_count || has_sum {
                let parts: Vec<String> = [
                    count.map(|c| format!("Count: {}", c)),
                    sum.map(|s| format!("Sum: {}", s)),
                    timestamp.map(|ts| format!("Time: {}", ts)),
                ]
                .into_iter()
                .flatten()
                .collect();
                println!("    {}", parts.join(" "));
            }
        }

        // Attributes (data point attributes)
        if let Some(kvs) = get_row_kvlist(row, "attributes") {
            print_kvlist(kvs, "Attributes", "  ");
        }
    }
}
