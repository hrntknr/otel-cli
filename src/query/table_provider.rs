use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

use crate::store::SharedStore;

use super::arrow_convert;

#[derive(Clone, Copy, Debug)]
pub enum TableKind {
    Traces,
    Logs,
    Metrics,
}

pub struct OtelTable {
    store: SharedStore,
    kind: TableKind,
    schema: SchemaRef,
}

impl fmt::Debug for OtelTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtelTable")
            .field("kind", &self.kind)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl OtelTable {
    pub fn new(store: SharedStore, kind: TableKind, schema: SchemaRef) -> Self {
        Self {
            store,
            kind,
            schema,
        }
    }
}

impl TableProvider for OtelTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        _state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        _filters: &'life3 [Expr],
        _limit: Option<usize>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Arc<dyn ExecutionPlan>>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        let projection = projection.cloned();
        Box::pin(async move {
            let store = self.store.read().await;
            let batch = match self.kind {
                TableKind::Traces => arrow_convert::traces_to_batch(&store),
                TableKind::Logs => arrow_convert::logs_to_batch(&store),
                TableKind::Metrics => arrow_convert::metrics_to_batch(&store),
            };
            drop(store);

            let partitions = if batch.num_rows() == 0 {
                vec![vec![]]
            } else {
                vec![vec![batch]]
            };
            Ok(MemorySourceConfig::try_new_exec(
                &partitions,
                self.schema.clone(),
                projection,
            )? as Arc<dyn ExecutionPlan>)
        })
    }
}

impl fmt::Display for OtelTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OtelTable({:?})", self.kind)
    }
}
