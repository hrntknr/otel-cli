use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;

use crate::store::SharedStore;

use super::arrow_schema;
use super::table_provider::{OtelTable, TableKind};

pub fn create_context(store: SharedStore) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_table(
        "traces",
        Arc::new(OtelTable::new(
            store.clone(),
            TableKind::Traces,
            arrow_schema::traces_schema(),
        )),
    )
    .expect("failed to register traces table");
    ctx.register_table(
        "logs",
        Arc::new(OtelTable::new(
            store.clone(),
            TableKind::Logs,
            arrow_schema::logs_schema(),
        )),
    )
    .expect("failed to register logs table");
    ctx.register_table(
        "metrics",
        Arc::new(OtelTable::new(
            store,
            TableKind::Metrics,
            arrow_schema::metrics_schema(),
        )),
    )
    .expect("failed to register metrics table");
    ctx
}

pub async fn execute_sql(
    ctx: &SessionContext,
    sql: &str,
) -> std::result::Result<Vec<RecordBatch>, String> {
    let df = ctx.sql(sql).await.map_err(df_err_to_string)?;
    df.collect().await.map_err(df_err_to_string)
}

fn df_err_to_string(e: DataFusionError) -> String {
    e.to_string()
}
