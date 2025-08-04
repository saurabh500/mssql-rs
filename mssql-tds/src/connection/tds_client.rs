// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::HashMap;

use crate::datatypes::sql_string::SqlString;
use crate::datatypes::sqltypes::SqlType;
use crate::error::Error::UsageError;
use crate::message::parameters::rpc_parameters::{
    RpcParameter, StatusFlags, build_parameter_list_string,
};
use crate::message::rpc::{RpcProcs, RpcType, SqlRpc};
use crate::message::transaction_management::{
    CreateTxnParams, TransactionIsolationLevel, TransactionManagementRequest,
    TransactionManagementType,
};
use crate::query::result::ReturnValue;
use crate::token::tokens::SqlCollation;
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
use async_trait::async_trait;
use tracing::{info, instrument};

use crate::{
    core::{CancelHandle, TdsResult},
    query::metadata::ColumnMetadata,
};

#[derive(Debug)]
pub struct TdsClient {
    pub(crate) transport: Box<NetworkTransport>,
    pub(crate) negotiated_settings: NegotiatedSettings,
    pub(crate) execution_context: ExecutionContext,

    // pub(crate) batch_result: Option<BatchResult<'static>>,
    pub(crate) current_metadata: Option<ColMetadataToken>,
    count_map: HashMap<CurrentCommand, usize>,

    return_values: Vec<ReturnValue>,
    current_result_set_has_been_read_till_end: bool,
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
            current_result_set_has_been_read_till_end: false,
        }
    }

    pub fn get_collation(&self) -> SqlCollation {
        self.negotiated_settings.database_collation
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
    #[instrument(skip(self), level = "info")]
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

        let metadata = self.move_to_column_metadata().await?;
        // No metadata means no rows were returned, so we set has_open_batch to false.
        if metadata.is_none() {
            self.execution_context.set_has_open_batch(false);
        } else {
            self.current_metadata = metadata;

            self.execution_context.set_has_open_batch(true);
        }
        Ok(())
    }

    // Executes a stored procedure with the given proc_id and parameters.
    // The parameters can be either positional or named.
    #[instrument(skip(self), level = "info")]
    pub async fn execute_sp_executesql(
        &mut self,
        sql: String,
        named_params: Vec<RpcParameter>,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        };

        let database_collation = self.negotiated_settings.database_collation;

        let sql_statement_value =
            SqlType::NVarcharMax(Some(SqlString::from_utf8_string(sql.clone())));

        // Create the parameter list for sp_execute_sql
        let statement_parameter = RpcParameter::new(None, StatusFlags::NONE, sql_statement_value);

        // Build the comma separated list of parameters
        let mut params_list_as_string = String::new();

        build_parameter_list_string(&named_params, &mut params_list_as_string);

        let params_as_sql_string = SqlType::NVarcharMax(Some(SqlString::from_utf8_string(
            params_list_as_string.clone(),
        )));

        let params_parameter = RpcParameter::new(None, StatusFlags::NONE, params_as_sql_string);

        // Create the parameter list for positional parameters of sp_execute_sql.
        // These could be named parameters as well, but we want to avoid sending the name
        // to send less data over the wire.
        let positional_parameters_vec = vec![statement_parameter, params_parameter];
        let positional_parameters = Some(positional_parameters_vec);

        // Build the RPC request.
        let rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::ExecuteSql),
            positional_parameters,
            Some(named_params),
            &database_collation,
            &self.execution_context,
        );

        let mut packet_writer =
            rpc.create_packet_writer(self.transport.as_writer(), timeout_sec, cancel_handle);
        rpc.serialize(&mut packet_writer).await?;

        let metadata = self.move_to_column_metadata().await?;
        // No metadata means no rows were returned, so we set has_open_batch to false.
        if metadata.is_none() {
            self.execution_context.set_has_open_batch(false);
            self.current_result_set_has_been_read_till_end = true;
        } else {
            self.current_metadata = metadata;
            self.current_result_set_has_been_read_till_end = false;
            self.execution_context.set_has_open_batch(true);
        }
        Ok(())
    }

    /// Executes a stored procedure with the given name and parameters.
    #[instrument(skip(self), level = "info")]
    pub async fn execute_stored_procedure(
        &mut self,
        stored_procedure_name: String,
        positional_parameters: Option<Vec<RpcParameter>>,
        named_parameters: Option<Vec<RpcParameter>>,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(crate::error::Error::UsageError(
                ALREADY_EXECUTING_ERROR.to_string(),
            ));
        };
        self.return_values.clear();
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

        let metadata = self.move_to_column_metadata().await?;
        // No metadata means no rows were returned, so we set has_open_batch to false.
        if metadata.is_none() {
            self.execution_context.set_has_open_batch(false);
            self.current_result_set_has_been_read_till_end = true;
        } else {
            self.current_metadata = metadata;
            self.current_result_set_has_been_read_till_end = false;
            self.execution_context.set_has_open_batch(true);
        }
        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    async fn drain_rows(&mut self) -> TdsResult<()> {
        if self.maybe_has_unread_rows() {
            // Drain the current result set.
            while let Some(row) = self.get_next_row().await? {
                info!("Consuming row while draining result set {:?}", row.len());
            }
        }
        Ok(())
    }

    async fn drain_stream(&mut self) -> TdsResult<()> {
        loop {
            let token = self
                .transport
                .receive_token(&ParserContext::None(()), None, None)
                .await?;
            match token {
                Tokens::Done(done) | Tokens::DoneProc(done) | Tokens::DoneInProc(done) => {
                    info!(?done);
                    info!(?done.status);
                    if !done.has_more() {
                        break;
                    }
                }
                Tokens::EnvChange(t1) => {
                    self.execution_context.capture_change_property(&t1)?;
                }
                _ => {
                    info!(?token);
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self), level = "debug", name = "move_to_column_metadata")]
    pub(crate) async fn move_to_column_metadata(&mut self) -> TdsResult<Option<ColMetadataToken>> {
        let parser_context = ParserContext::None(());
        let mut col_metadata: Option<ColMetadataToken> = None;

        while let Ok(token) = self
            .transport
            .receive_token(&parser_context, None, None)
            .await
        {
            match token {
                Tokens::ColMetadata(md) => {
                    info!(?md);
                    col_metadata = Some(md);
                    self.current_result_set_has_been_read_till_end = false;
                    break;
                }
                Tokens::DoneInProc(done) | Tokens::DoneProc(done) | Tokens::Done(done) => {
                    info!(?done);

                    let count = self.count_map.entry(done.cur_cmd).or_insert(0);
                    *count += done.row_count as usize;
                    self.current_result_set_has_been_read_till_end = true;
                    if !done.has_more() {
                        self.execution_context.set_has_open_batch(false);
                        break;
                    }
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
                Tokens::ReturnStatus(return_status) => {
                    info!("Received return_status token: {:?}", return_status);
                    continue;
                }
                Tokens::Error(error_token) => {
                    info!(?error_token);
                    self.drain_stream().await?;
                    // Drain the stream till the done token with no more rows.
                    return Err(crate::error::Error::SqlServerError {
                        message: error_token.message.clone(),
                        state: error_token.state,
                        class: error_token.severity as i32,
                        number: error_token.number,
                        server_name: Some(error_token.server_name.clone()),
                        proc_name: Some(error_token.proc_name.clone()),
                        line_number: Some(error_token.line_number as i32),
                    });
                }
                Tokens::Info(info_token) => {
                    info!(?info_token);
                    continue;
                }
                _ => {
                    info!("move_to_column_metadata: {:?}", token);
                    return Err(UsageError(format!(
                        "Unexpected token while moving to column metadata: {token:?}"
                    )));
                }
            }
        }
        Ok(col_metadata)
    }

    /// This functions returns to the next row in the result set.
    /// If there are no more rows, it returns None.
    #[instrument(skip(self), level = "info")]
    pub(crate) async fn get_next_row(&mut self) -> TdsResult<Option<Vec<ColumnValues>>> {
        if self.current_metadata.is_none() {
            return Err(UsageError(
                "No metadata found while fetching the next row. Have you called the execute method or was the query supposed to return resultset?".to_string(),
            ));
        }
        let parser_context = ParserContext::ColumnMetadata(self.current_metadata.clone().unwrap());
        let mut result: Option<Vec<ColumnValues>> = None;
        loop {
            let token = self
                .transport
                .receive_token(&parser_context, None, None)
                .await?;

            match token {
                Tokens::Row(row) | Tokens::NbcRow(row) => {
                    info!("Row Received");
                    result = Some(row.all_values);
                    break;
                }
                Tokens::DoneInProc(done) | Tokens::DoneProc(done) | Tokens::Done(done) => {
                    info!("done while get_next_row: {:?}", done);

                    let count = self.count_map.entry(done.cur_cmd).or_insert(0);
                    *count += done.row_count as usize;

                    self.current_result_set_has_been_read_till_end = true;
                    if !done.has_more() {
                        // Token stream is terminated. Save this information.
                        info!("No more rows for current command: {:?}", done.cur_cmd);
                        self.execution_context.set_has_open_batch(false);
                    }
                    break;
                }
                Tokens::Order(order_token) => {
                    // Ignore.
                    info!(?order_token);
                    continue;
                }
                Tokens::ReturnValue(return_value_token) => {
                    let return_value = return_value_token.into();
                    self.return_values.push(return_value);
                    continue;
                }
                _ => {
                    unreachable!("Unexpected Token while finding the next row. {:?}", token);
                }
            }
        }

        Ok(result)
    }

    /// Gets the return values collected so far.
    pub fn get_return_values(&self) -> Vec<ReturnValue> {
        self.return_values.clone()
    }

    #[instrument(skip(self), level = "info")]
    pub async fn close_query(&mut self) -> TdsResult<()> {
        if !self.execution_context.has_open_batch() {
            return Ok(());
        }
        // call next row to consume any remaining tokens
        while self.move_to_next().await? {}
        info!("No more rows to consume.");

        // Reset the current metadata and return values.
        self.current_metadata = None;
        self.return_values.clear();
        self.execution_context.set_has_open_batch(false);
        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn close_connection(&mut self) -> TdsResult<()> {
        self.transport.close_transport().await?;
        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn begin_transaction(
        &mut self,
        isolation_level: TransactionIsolationLevel,
    ) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(
                "Cannot begin transaction while another batch is executing.".to_string(),
            ));
        }
        let transaction_params = TransactionManagementType::Begin(CreateTxnParams {
            level: isolation_level,
            name: None,
        });
        let transaction =
            TransactionManagementRequest::new(transaction_params, &self.execution_context);
        let mut packet_writer =
            transaction.create_packet_writer(self.transport.as_writer(), None, None);
        transaction.serialize(&mut packet_writer).await?;

        self.consume_transaction_response().await?;

        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn commit_transaction(&mut self) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(
                "Cannot commit transaction while another batch is executing.".to_string(),
            ));
        }
        let transaction = TransactionManagementRequest::new(
            TransactionManagementType::Commit {
                name: None,
                create_txn_params: None,
            },
            &self.execution_context,
        );
        let mut packet_writer =
            transaction.create_packet_writer(self.transport.as_writer(), None, None);
        transaction.serialize(&mut packet_writer).await?;

        self.consume_transaction_response().await?;

        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn rollback_transaction(&mut self) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(
                "Cannot rollback transaction while another batch is executing.".to_string(),
            ));
        }
        let transaction = TransactionManagementRequest::new(
            TransactionManagementType::Rollback {
                name: None,
                create_txn_params: None,
            },
            &self.execution_context,
        );
        let mut packet_writer =
            transaction.create_packet_writer(self.transport.as_writer(), None, None);
        transaction.serialize(&mut packet_writer).await?;

        self.consume_transaction_response().await?;

        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub(crate) async fn consume_transaction_response(&mut self) -> TdsResult<()> {
        loop {
            let token = self
                .transport
                .receive_token(&ParserContext::None(()), None, None)
                .await?;
            match token {
                Tokens::DoneInProc(done) | Tokens::DoneProc(done) | Tokens::Done(done) => {
                    info!("done while consume_transaction_response: {:?}", done);

                    let count = self.count_map.entry(done.cur_cmd).or_insert(0);
                    *count += done.row_count as usize;

                    if !done.has_more() {
                        // Token stream is terminated. Save this information.
                        info!("No more rows for current command: {:?}", done.cur_cmd);
                    }
                    break;
                }
                Tokens::EnvChange(env_change) => {
                    info!(?env_change);
                    self.execution_context
                        .capture_change_property(&env_change)?;
                    continue;
                }
                _ => {
                    unreachable!(
                        "Unexpected token while reading transaction request response. {:?}",
                        token
                    );
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl ResultSet for TdsClient {
    fn get_metadata(&self) -> &Vec<ColumnMetadata> {
        if self.current_metadata.is_none() {
            unreachable!("No metadata found. Is there a query executed?");
        }
        &self.current_metadata.as_ref().unwrap().columns
    }

    #[instrument(skip(self), level = "info")]
    async fn next_row(&mut self) -> TdsResult<Option<Vec<ColumnValues>>> {
        if self.maybe_has_unread_rows() {
            // If there are rows available, fetch the next row.
            self.get_next_row().await
        } else {
            Ok(None)
        }
    }

    fn maybe_has_unread_rows(&self) -> bool {
        !self.current_result_set_has_been_read_till_end
    }

    #[instrument(skip(self), level = "info")]
    async fn close(&mut self) -> TdsResult<()> {
        self.close_query().await
    }
}

#[async_trait]
impl ResultSetClient for TdsClient {
    fn get_current_resultset(&mut self) -> Option<&mut TdsClient> {
        if self.execution_context.has_open_batch() {
            Some(self)
        } else {
            None
        }
    }

    #[instrument(skip(self), level = "info")]
    async fn move_to_next(&mut self) -> TdsResult<bool> {
        if !self.execution_context.has_open_batch() {
            return Ok(false);
        }
        // Drain the current result set.
        if self.maybe_has_unread_rows() {
            self.drain_rows().await?;
        }

        info!("Moving to next result set...");

        let has_open_batch = self.execution_context.has_open_batch();
        info!("Has open batch: {}", has_open_batch);
        if !has_open_batch {
            return Ok(false);
        }
        let metadata_token = self.move_to_column_metadata().await?;

        match metadata_token {
            Some(metadata) => {
                self.current_metadata = Some(metadata);
                self.execution_context.set_has_open_batch(true);
                self.current_result_set_has_been_read_till_end = false;
                Ok(true)
            }
            None => {
                // No metadata means no more result sets.
                self.execution_context.set_has_open_batch(false);
                self.current_metadata = None;
                self.current_result_set_has_been_read_till_end = true;
                Ok(false)
            }
        }
    }
}

#[async_trait]
pub trait ResultSet {
    /// Returns the metadata of the result set.
    /// This metadata includes information about the columns in the result set.
    fn get_metadata(&self) -> &Vec<ColumnMetadata>;

    /// Returns the next row of data as a vector of column values.
    /// If there is no more data, it returns None.
    async fn next_row(&mut self) -> TdsResult<Option<Vec<ColumnValues>>>;

    fn maybe_has_unread_rows(&self) -> bool;

    /// Iterates over the result set, and marks it as closed. After calling close, the next_row method,
    /// will always return None.
    async fn close(&mut self) -> TdsResult<()>;
}

#[async_trait]
pub trait ResultSetClient<T = TdsClient> {
    /// Returns the current result set on the client.
    /// Execution of query positions the client at the first result set.
    /// If we have read all the results from the current result set,
    /// this method will return None.
    fn get_current_resultset(&mut self) -> Option<&mut T>;

    /// Moves to the next result set, if available.
    /// Returns true if there is a next result set, false otherwise.
    /// The current_resultset will be closed and if the next result set is available,
    /// it will be set as the current result set.
    /// If there is no next result set, the current result set will be closed and
    /// the method will return false.
    async fn move_to_next(&mut self) -> TdsResult<bool>;
}
