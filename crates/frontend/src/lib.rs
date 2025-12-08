use anyhow::Result;
use datafusion::prelude::SessionContext;
use datafusion_postgres::{ServerOptions, auth::AuthManager, serve};

use async_trait::async_trait;
use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

#[derive(Debug, Default)]
pub struct LSMTableProvider {}

impl LSMTableProvider {}

// https://datafusion.apache.org/library-user-guide/custom-table-providers.html
#[async_trait]
impl TableProvider for LSMTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        todo!()
    }
}

pub async fn serve_postgres(port: u16) -> Result<()> {
    let session_context = Arc::new(SessionContext::new());
    let provider = LSMTableProvider {};
    session_context.register_table("database", Arc::new(provider))?;
    let server_options = ServerOptions::new()
        .with_host("127.0.0.1".to_string())
        .with_port(port);
    let auth_manager = Arc::new(AuthManager::default());
    serve(session_context, &server_options, auth_manager).await?;
    Ok(())
}
