use std::collections::HashMap;

use crate::error::Error::UsageError;
use crate::message::parameters::rpc_parameters::RpcParameter;
use crate::message::rpc::{RpcType, SqlRpc};
use crate::query::result::ReturnValue;
use crate::{
    connection::{
        tds_connection::{ALREADY_EXECUTING_ERROR, ExecutionContext},
        transport::network_transport::NetworkTransport,
    },
    datatypes::column_values::ColumnValues,
    handler::handler_factory::NegotiatedSettings,
    message::{batch::SqlBatch, messages::Request},
    read_write::{
        packet_reader::TdsPacketReader,
        reader_writer::NetworkReaderWriter,
        token_stream::{ParserContext, TdsTokenStreamReader},
    },
    token::tokens::{ColMetadataToken, CurrentCommand, Tokens},
};
use tracing::info;

use crate::{
    core::{CancelHandle, TdsResult},
    query::metadata::ColumnMetadata,
};

pub struct TdsClient {
    pub(crate) transport: Box<NetworkTransport>,
    pub(crate) negotiated_settings: NegotiatedSettings,
    pub(crate) execution_context: ExecutionContext,

    // pub(crate) batch_result: Option<BatchResult<'static>>,
    current_metadata: Option<ColMetadataToken>,
    count_map: HashMap<CurrentCommand, usize>,

    return_values: Vec<ReturnValue>,
}

impl TdsClient {
    pub(crate) fn new(
        transport: Box<NetworkTransport>,
        negotiated_settings: NegotiatedSettings,
        execution_context: ExecutionContext,
    ) -> Self {
        Self {
            transport,
            negotiated_settings,
            execution_context,
            current_metadata: None,
            count_map: HashMap::new(),
            return_values: Vec::new(),
        }
    }

    pub(crate) fn get_transport(&self) -> &NetworkTransport {
        &self.transport
    }

    pub(crate) fn get_negotiated_settings(&self) -> &NegotiatedSettings {
        &self.negotiated_settings
    }

    pub(crate) fn get_execution_context(&self) -> &ExecutionContext {
        &self.execution_context
    }

    /// Executes a SQL command (batch) against the server.
    pub async fn execute(
        &mut self,
        sql_command: String,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(crate::error::Error::UsageError(
                ALREADY_EXECUTING_ERROR.to_string(),
            ));
        };
        self.transport.reset_reader();
        let batch = SqlBatch::new(sql_command, &self.execution_context);
        // let start = Instant::now();
        let mut packet_writer =
            batch.create_packet_writer(self.transport.as_writer(), timeout_sec, cancel_handle);
        batch.serialize(&mut packet_writer).await?;

        let mut command_count_map = HashMap::new();
        let metadata =
            Self::move_to_column_metadata(&mut self.transport, &mut command_count_map).await?;
        // No metadata means no rows were returned, so we set has_open_batch to false.
        if metadata.is_none() {
            self.execution_context.set_has_open_batch(false);
        } else {
            self.current_metadata = metadata;

            self.execution_context.set_has_open_batch(true);
        }
        Ok(())
    }

    /// Executes a stored procedure with the given name and parameters.
    pub async fn execute_stored_procedure(
        &mut self,
        stored_procedure_name: String,
        positional_parameters: Option<&Vec<RpcParameter<'_>>>,
        named_parameters: Option<&Vec<RpcParameter<'_>>>,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(crate::error::Error::UsageError(
                ALREADY_EXECUTING_ERROR.to_string(),
            ));
        };
        self.transport.reset_reader();
        let database_collation = self.negotiated_settings.database_collation;

        let rpc = SqlRpc::new(
            RpcType::Named(stored_procedure_name),
            positional_parameters,
            named_parameters,
            &database_collation,
            &self.execution_context,
        );

        let mut packet_writer =
            rpc.create_packet_writer(self.transport.as_writer(), timeout_sec, cancel_handle);
        rpc.serialize(&mut packet_writer).await?;

        let mut command_count_map = HashMap::new();
        let metadata =
            Self::move_to_column_metadata(&mut self.transport, &mut command_count_map).await?;
        // No metadata means no rows were returned, so we set has_open_batch to false.
        if metadata.is_none() {
            self.execution_context.set_has_open_batch(false);
        } else {
            self.current_metadata = metadata;

            self.execution_context.set_has_open_batch(true);
        }
        Ok(())
    }

    /// Gets the next row of data, even from across result sets.
    pub async fn next_row(&mut self) -> TdsResult<Option<Vec<ColumnValues>>> {
        if self.current_metadata.is_none() {
            return Err(UsageError(
                "No metadata found while fetching the next row. Have you called the execute method or was the query supposed to return resultset?".to_string(),
            ));
        }
        let mut parser_context =
            ParserContext::ColumnMetadata(self.current_metadata.clone().unwrap());
        let mut result: Option<Vec<ColumnValues>> = None;

        while let Ok(token) = self
            .transport
            .receive_token(&parser_context, None, None)
            .await
        {
            match token {
                Tokens::ColMetadata(md) => {
                    info!(?md);
                    self.current_metadata = Some(md);
                    parser_context =
                        ParserContext::ColumnMetadata(self.current_metadata.clone().unwrap());
                    // Don't break on col metadata. Store and move on.
                }
                Tokens::DoneInProc(done) | Tokens::DoneProc(done) | Tokens::Done(done) => {
                    info!(?done);

                    let count = self.count_map.entry(done.cur_cmd).or_insert(0);
                    *count += done.row_count as usize;

                    if !done.has_more() {
                        info!("No more rows for current command: {:?}", done.cur_cmd);
                        self.execution_context.set_has_open_batch(false);
                        break;
                    }
                }
                Tokens::Row(row) | Tokens::NbcRow(row) => {
                    info!("Row Received");
                    result = Some(row.all_values);
                    break;
                }
                Tokens::EnvChange(env_change) => {
                    info!(?env_change);
                    self.execution_context
                        .capture_change_property(&env_change)?;
                }
                Tokens::ReturnValue(return_value_token) => {
                    let return_value = return_value_token.into();
                    self.return_values.push(return_value);
                }
                _ => {
                    info!(?token);
                }
            }
        }
        Ok(result)
    }

    pub fn get_metdata(&self) -> TdsResult<Vec<ColumnMetadata>> {
        if self.current_metadata.is_none() {
            return Err(crate::error::Error::UsageError(
                "No metadata found. Is there a query executed?".to_string(),
            ));
        }
        Ok(self.current_metadata.clone().unwrap_or_default().columns)
    }

    pub(crate) async fn move_to_column_metadata(
        token_stream_reader: &mut NetworkTransport,
        count_map: &mut HashMap<CurrentCommand, usize>,
    ) -> TdsResult<Option<ColMetadataToken>> {
        // Implementation for moving to the first column metadata
        let parser_context = ParserContext::None(());
        let mut col_metadata: Option<ColMetadataToken> = None;

        while let Ok(token) = token_stream_reader
            .receive_token(&parser_context, None, None)
            .await
        {
            match token {
                Tokens::ColMetadata(md) => {
                    info!(?md);
                    col_metadata = Some(md);
                    break;
                }
                Tokens::DoneInProc(done) | Tokens::DoneProc(done) | Tokens::Done(done) => {
                    info!(?done);

                    let count = count_map.entry(done.cur_cmd).or_insert(0);
                    *count += done.row_count as usize;

                    if !done.has_more() {
                        break;
                    }
                }
                _ => {
                    info!(?token);
                }
            }
        }
        Ok(col_metadata)
    }

    /// Gets the return values collected so far.
    pub fn get_return_values(&self) -> &Vec<ReturnValue> {
        &self.return_values
    }

    pub async fn close_query(&mut self) -> TdsResult<()> {
        if !self.execution_context.has_open_batch() {
            return Err(crate::error::Error::UsageError(
                "No open batch to close.".to_string(),
            ));
        }
        // call next row to consume any remaining tokens
        while let Ok(Some(_)) = self.next_row().await {
            // Consume all remaining rows until no more rows are available.
        }

        // Reset the current metadata and return values.
        self.current_metadata = None;
        self.return_values.clear();
        self.execution_context.set_has_open_batch(false);
        Ok(())
    }

    pub async fn close_connection(&mut self) -> TdsResult<()> {
        self.transport.close_transport().await?;
        Ok(())
    }
}
