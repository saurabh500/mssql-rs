// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::HashMap;

use crate::datatypes::sql_string::SqlString;
use crate::datatypes::sqltypes::SqlType;
use crate::error::Error::UsageError;
use crate::message::bulk_load::BulkLoadMessage;
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
        execution_context::{ALREADY_EXECUTING_ERROR, ExecutionContext},
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
use std::time::{Duration, Instant};

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

    /// The remaining request timeout for operations. This is updated after each token read.
    remaining_request_timeout: Option<Duration>,

    /// The cancel handle for this client. Used to cancel operations.
    cancel_handle: Option<CancelHandle>,
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
            remaining_request_timeout: None,
            cancel_handle: None,
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

    /// Updates the remaining timeout by subtracting the elapsed time.
    fn update_remaining_timeout(&mut self, start: Instant) {
        self.remaining_request_timeout = self.remaining_request_timeout.map(|t| {
            let elapsed = start.elapsed();
            if elapsed > t {
                Duration::ZERO
            } else {
                t.saturating_sub(elapsed)
            }
        });
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

        // Store timeout and cancel handle for this operation
        self.remaining_request_timeout = timeout_sec.map(|secs| Duration::from_secs(secs as u64));
        self.cancel_handle = cancel_handle.map(|handle| handle.child_handle());

        self.transport.reset_reader();
        let batch = SqlBatch::new(sql_command, &self.execution_context);
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
    #[instrument(skip(self, named_params), level = "info")]
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

        // Store timeout and cancel handle for this operation
        self.remaining_request_timeout = timeout_sec.map(|secs| Duration::from_secs(secs as u64));
        self.cancel_handle = cancel_handle.map(|handle| handle.child_handle());

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

    /// Fetches table metadata from SQL Server by querying the table with TOP 0.
    ///
    /// This method queries the destination table to get the exact column metadata
    /// (including TDS types) that SQL Server expects. This matches the .NET SqlBulkCopy
    /// behavior which queries the table schema before sending bulk data.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The fully qualified table name (e.g., "dbo.TableName")
    /// * `timeout_sec` - Optional timeout in seconds for the query
    /// * `cancel_handle` - Optional cancellation handle
    ///
    /// # Returns
    ///
    /// A `ColMetadataToken` containing the column metadata from the server.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table doesn't exist
    /// - Network errors occur
    /// - Timeout occurs
    /// - Operation is cancelled
    #[instrument(skip(self), level = "info")]
    pub async fn fetch_table_metadata(
        &mut self,
        table_name: &str,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<ColMetadataToken> {
        // Query with TOP 0 to get metadata without fetching actual rows
        let query = format!("SELECT TOP 0 * FROM {}", table_name);
        
        eprintln!("DEBUG: Fetching table metadata with query: {}", query);
        
        // Execute the query
        self.execute(query, timeout_sec, cancel_handle).await?;
        
        // Get the metadata from the result
        let metadata = self.current_metadata.clone().ok_or_else(|| {
            UsageError(format!("Failed to fetch metadata for table {}", table_name))
        })?;
        
        eprintln!("DEBUG: Fetched {} columns from table metadata", metadata.columns.len());
        for (i, col) in metadata.columns.iter().enumerate() {
            eprintln!("DEBUG:   Column {}: name='{}', tds_type=0x{:02X}, nullable={}", 
                i, col.column_name, col.data_type as u8, col.is_nullable());
        }
        
        // Close the query to free up the connection
        self.close_query().await?;
        
        Ok(metadata)
    }

    /// Executes a bulk load operation.
    ///
    /// This method implements the TDS bulk load protocol following the proper request-response pattern:
    /// 1. Send INSERT BULK command → Read response (DONE token with cur_cmd=0xFD)
    /// 2. Send bulk data (COLMETADATA + ROW tokens) → Read response (DONE token with cur_cmd=0xF0)
    ///
    /// This two-phase approach matches .NET SqlBulkCopy behavior and eliminates the need for
    /// special DONE token filtering. Each operation is acknowledged by the server before
    /// proceeding to the next step.
    ///
    /// # Arguments
    ///
    /// * `message` - The bulk load message containing table name, metadata, and rows
    /// * `timeout_sec` - Optional timeout in seconds for the operation
    /// * `cancel_handle` - Optional cancellation handle
    ///
    /// # Returns
    ///
    /// The number of rows affected (inserted) as reported by the SQL Server DONE token.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - There's an open batch already executing
    /// - Network errors occur during transmission
    /// - SQL Server returns an error (constraints, type mismatches, etc.)
    /// - Timeout occurs
    /// - Operation is cancelled
    #[instrument(skip(self, message), level = "info")]
    pub(crate) async fn execute_bulk_load(
        &mut self,
        message: BulkLoadMessage,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<u64> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        }

        // Store timeout and cancel handle for this operation
        self.remaining_request_timeout = timeout_sec.map(|secs| Duration::from_secs(secs as u64));
        self.cancel_handle = cancel_handle.map(|handle| handle.child_handle());

        self.transport.reset_reader();

        // STEP 1: Send INSERT BULK command and consume response
        // Uses the standard batch send/consume pattern (reuses send_batch_and_consume_response)
        let insert_bulk_command = message.build_insert_bulk_command();
        self.send_batch_and_consume_response(insert_bulk_command, timeout_sec, cancel_handle).await?;

        // STEP 2: Send the COLMETADATA and row data
        let mut packet_writer =
            message.create_packet_writer(self.transport.as_writer(), timeout_sec, cancel_handle);
        message.serialize(&mut packet_writer).await?;

        // STEP 3: Read the final response with row count
        let rows_affected = self.consume_done_token().await?;

        Ok(rows_affected)
    }

    /// Consumes response tokens until a DONE token is received.
    /// Returns the row count from the DONE token.
    /// 
    /// This helper method implements the standard TDS response consumption pattern,
    /// handling INFO, ERROR, and DONE tokens appropriately.
    async fn consume_done_token(&mut self) -> TdsResult<u64> {
        let parser_context = ParserContext::None(());
        let mut rows_affected: u64 = 0;

        loop {
            let start = Instant::now();
            let token = self
                .transport
                .receive_token(
                    &parser_context,
                    self.remaining_request_timeout,
                    self.cancel_handle.as_ref(),
                )
                .await?;
            self.update_remaining_timeout(start);

            match token {
                Tokens::Done(done) | Tokens::DoneProc(done) | Tokens::DoneInProc(done) => {
                    info!("Done token: {:?}", done);
                    
                    rows_affected = done.row_count;
                    
                    // Stop when we receive a DONE token without the MORE flag
                    if !done.has_more() {
                        break;
                    }
                }
                Tokens::Error(error_token) => {
                    info!(?error_token);
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
                    // Informational message from server
                    info!(?info_token);
                    continue;
                }
                Tokens::EnvChange(env_change) => {
                    // Handle environment changes
                    info!(?env_change);
                    self.execution_context
                        .capture_change_property(&env_change)?;
                    continue;
                }
                _ => {
                    // Unexpected token
                    info!("Unexpected token during bulk load: {:?}", token);
                    return Err(UsageError(format!(
                        "Unexpected token while executing bulk load: {token:?}"
                    )));
                }
            }
        }

        Ok(rows_affected)
    }

    /// Sends a SQL batch and consumes the response without expecting column metadata.
    /// This is used for commands that don't return result sets (DML statements, etc.).
    /// 
    /// Returns the row count from the DONE token.
    async fn send_batch_and_consume_response(
        &mut self,
        sql_command: String,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<u64> {
        let batch = SqlBatch::new(sql_command, &self.execution_context);
        let mut packet_writer =
            batch.create_packet_writer(self.transport.as_writer(), timeout_sec, cancel_handle);
        batch.serialize(&mut packet_writer).await?;

        // Consume the response
        self.consume_done_token().await
    }

    /// Executes a stored procedure with the given name and parameters.
    #[instrument(skip(self, positional_parameters, named_parameters), level = "info")]
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

        // Store timeout and cancel handle for this operation
        self.remaining_request_timeout = timeout_sec.map(|secs| Duration::from_secs(secs as u64));
        self.cancel_handle = cancel_handle.map(|handle| handle.child_handle());

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

    /// Prepares a SQL statement for execution and returns the prepared handle.
    /// This uses `sp_prepare` under the hood.
    #[instrument(skip(self, named_params), level = "info")]
    pub async fn execute_sp_prepare(
        &mut self,
        sql: String,
        named_params: Vec<RpcParameter>,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<i32> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        };

        // Store timeout and cancel handle for this operation
        self.remaining_request_timeout = timeout_sec.map(|secs| Duration::from_secs(secs as u64));
        self.cancel_handle = cancel_handle.map(|handle| handle.child_handle());

        self.return_values.clear();
        self.transport.reset_reader();

        let database_collation = self.negotiated_settings.database_collation;

        let sql_statement_value = SqlType::NVarcharMax(Some(SqlString::from_utf8_string(sql)));

        // Create the parameter list for sp_prepare
        let execute_sql_statement_parameter =
            RpcParameter::new(None, StatusFlags::NONE, sql_statement_value);

        // Build the comma separated list of parameters
        let mut params_list_as_string = String::new();

        build_parameter_list_string(&named_params, &mut params_list_as_string);

        let params_as_sql_string =
            SqlType::NVarcharMax(Some(SqlString::from_utf8_string(params_list_as_string)));

        let params_parameter = RpcParameter::new(None, StatusFlags::NONE, params_as_sql_string);

        let output_handler_value = SqlType::Int(None);

        let output_handler_parameter = RpcParameter::new(
            None,
            StatusFlags::BY_REF_VALUE, // Output parameter
            output_handler_value,
        );

        // Create the parameter list for positional parameters of sp_prepare.
        let positional_parameters_vec = vec![
            output_handler_parameter,
            params_parameter,
            execute_sql_statement_parameter,
        ];
        let positional_parameters = Some(positional_parameters_vec);

        // Build the RPC request.
        let rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::Prepare),
            positional_parameters,
            Some(named_params),
            &database_collation,
            &self.execution_context,
        );

        let mut packet_writer =
            rpc.create_packet_writer(self.transport.as_writer(), timeout_sec, cancel_handle);
        rpc.serialize(&mut packet_writer).await?;

        // Drain to completion to get output parameters
        self.drain_stream().await?;

        // We need to get the return value, and then extract the handle from it.
        if self.return_values.len() == 1 {
            let returned_parameter = self.return_values.first().unwrap();
            if let ColumnValues::Int(handle) = &returned_parameter.value {
                Ok(*handle)
            } else {
                Err(crate::error::Error::ProtocolError(
                    "Expected an integer value".to_string(),
                ))
            }
        } else {
            Err(crate::error::Error::ProtocolError(
                "Expected exactly one output parameter".to_string(),
            ))
        }
    }

    /// Unprepares a previously prepared statement using `sp_unprepare`.
    #[instrument(skip(self), level = "info")]
    pub async fn execute_sp_unprepare(
        &mut self,
        handle: i32,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        };

        // Store timeout and cancel handle for this operation
        self.remaining_request_timeout = timeout_sec.map(|secs| Duration::from_secs(secs as u64));
        self.cancel_handle = cancel_handle.map(|handle| handle.child_handle());

        self.transport.reset_reader();

        let database_collation = self.negotiated_settings.database_collation;

        let handle_value = SqlType::Int(Some(handle));
        let handle_parameter = RpcParameter::new(None, StatusFlags::NONE, handle_value);

        let positional_parameters_vec = vec![handle_parameter];
        let positional_parameters = Some(positional_parameters_vec);

        // Build the RPC request.
        let rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::Unprepare),
            positional_parameters,
            None,
            &database_collation,
            &self.execution_context,
        );

        let mut packet_writer =
            rpc.create_packet_writer(self.transport.as_writer(), timeout_sec, cancel_handle);
        rpc.serialize(&mut packet_writer).await?;

        // Drain the result set. A successful unprepare will not return any results.
        self.drain_stream().await?;
        Ok(())
    }

    /// Executes `sp_prepexec` which will prepare the statement for execution,
    /// return a result set as well as a prepared handle.
    #[instrument(skip(self, named_params), level = "info")]
    pub async fn execute_sp_prepexec(
        &mut self,
        sql: String,
        named_params: Vec<RpcParameter>,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        };

        // Store timeout and cancel handle for this operation
        self.remaining_request_timeout = timeout_sec.map(|secs| Duration::from_secs(secs as u64));
        self.cancel_handle = cancel_handle.map(|handle| handle.child_handle());

        self.return_values.clear();
        self.transport.reset_reader();

        let database_collation = self.negotiated_settings.database_collation;

        let sql_statement_value = SqlType::NVarcharMax(Some(SqlString::from_utf8_string(sql)));

        // Create the parameter list for sp_prepexec
        let statement_parameter = RpcParameter::new(None, StatusFlags::NONE, sql_statement_value);

        // Build the comma separated list of parameters
        let mut params_list_as_string = String::new();

        build_parameter_list_string(&named_params, &mut params_list_as_string);

        let params_as_sql_string =
            SqlType::NVarcharMax(Some(SqlString::from_utf8_string(params_list_as_string)));

        let params_parameter = RpcParameter::new(None, StatusFlags::NONE, params_as_sql_string);

        let handle_value = SqlType::Int(None);

        let handle_parameter = RpcParameter::new(None, StatusFlags::BY_REF_VALUE, handle_value);

        // Create the parameter list for positional parameters of sp_prepexec.
        let positional_parameters_list =
            vec![handle_parameter, params_parameter, statement_parameter];
        let positional_parameters = Some(positional_parameters_list);

        // Build the RPC request.
        let rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::PrepExec),
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

    /// Executes a previously prepared statement using `sp_execute`.
    #[instrument(skip(self, positional_parameters, named_parameters), level = "info")]
    pub async fn execute_sp_execute(
        &mut self,
        handle: i32,
        positional_parameters: Option<Vec<RpcParameter>>,
        named_parameters: Option<Vec<RpcParameter>>,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        };

        // Store timeout and cancel handle for this operation
        self.remaining_request_timeout = timeout_sec.map(|secs| Duration::from_secs(secs as u64));
        self.cancel_handle = cancel_handle.map(|handle| handle.child_handle());

        self.return_values.clear();
        self.transport.reset_reader();

        let database_collation = self.negotiated_settings.database_collation;

        let handle_value = SqlType::Int(Some(handle));
        let handle_parameter = RpcParameter::new(None, StatusFlags::NONE, handle_value);

        // Create the parameter list for positional parameters of sp_execute.
        let mut all_positional_parameters = vec![handle_parameter];

        if let Some(mut params) = positional_parameters {
            all_positional_parameters.append(&mut params);
        }
        let all_positional_parameters = Some(all_positional_parameters);

        // Build the RPC request.
        let rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::Execute),
            all_positional_parameters,
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
            let start = Instant::now();
            let token = self
                .transport
                .receive_token(
                    &ParserContext::None(()),
                    self.remaining_request_timeout,
                    self.cancel_handle.as_ref(),
                )
                .await?;
            self.update_remaining_timeout(start);

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
                Tokens::ReturnValue(return_value_token) => {
                    let return_value = return_value_token.into();
                    self.return_values.push(return_value);
                }
                Tokens::ReturnStatus(_return_status) => {
                    info!(?_return_status);
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

        loop {
            let start = Instant::now();
            let token = self
                .transport
                .receive_token(
                    &parser_context,
                    self.remaining_request_timeout,
                    self.cancel_handle.as_ref(),
                )
                .await?;
            self.update_remaining_timeout(start);
            match token {
                Tokens::ColMetadata(md) => {
                    info!(?md);
                    col_metadata = Some(md);
                    self.current_result_set_has_been_read_till_end = false;
                    break;
                }
                Tokens::DoneInProc(done) | Tokens::DoneProc(done) | Tokens::Done(done) => {
                    info!(
                        ?done,
                        "Received Done token with has_more={}",
                        done.has_more()
                    );

                    let count = self.count_map.entry(done.cur_cmd).or_insert(0);
                    *count += done.row_count as usize;
                    self.current_result_set_has_been_read_till_end = true;

                    if !done.has_more() {
                        // No more result sets - end of batch
                        info!("No more result sets (has_more=false), ending batch");
                        self.execution_context.set_has_open_batch(false);
                        break;
                    }

                    // has_more() is true - there are more result sets coming
                    // For DML operations (CREATE TABLE, INSERT, UPDATE, DELETE), there's no ColMetadata.
                    // The Done token represents the result, but we skip over it to find the next
                    // result set with ColMetadata (SELECT). This matches SQL Server behavior.
                    info!(
                        "More result sets available (has_more=true), continuing to look for ColMetadata"
                    );
                    continue;
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
            let start = Instant::now();
            let token = self
                .transport
                .receive_token(
                    &parser_context,
                    self.remaining_request_timeout,
                    self.cancel_handle.as_ref(),
                )
                .await?;
            self.update_remaining_timeout(start);

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
                Tokens::EnvChange(env_change) => {
                    // Handle environment changes during row iteration
                    info!(?env_change);
                    self.execution_context
                        .capture_change_property(&env_change)?;
                    continue;
                }
                Tokens::ReturnValue(return_value_token) => {
                    let return_value = return_value_token.into();
                    self.return_values.push(return_value);
                    continue;
                }
                Tokens::Error(error_token) => {
                    // SQL Server error occurred during row iteration
                    info!(?error_token);
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

    /// Retrieves a snapshot of the output parameters (including return values)
    /// that have been retrieved from the result stream.
    ///
    /// Returns `None` if there are no output parameters, otherwise returns
    /// a reference to the collected return values.
    pub fn retrieve_output_params(&self) -> TdsResult<Option<&Vec<ReturnValue>>> {
        if self.return_values.is_empty() {
            Ok(None)
        } else {
            Ok(Some(&self.return_values))
        }
    }

    #[instrument(skip(self), level = "info")]
    pub async fn close_query(&mut self) -> TdsResult<()> {
        if !self.execution_context.has_open_batch() {
            return Ok(());
        }
        // call next row to consume any remaining tokens
        while self.move_to_next().await? {}
        info!("No more rows to consume.");

        // Reset the current metadata, return values, and timeout/cancel state.
        self.current_metadata = None;
        self.return_values.clear();
        self.remaining_request_timeout = None;
        self.cancel_handle = None;
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
        name: Option<String>,
    ) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(
                "Cannot begin transaction while another batch is executing.".to_string(),
            ));
        }
        let transaction_params = TransactionManagementType::Begin(CreateTxnParams {
            level: isolation_level,
            name,
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
    pub async fn save_transaction(&mut self, name: String) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(
                "Cannot save transaction while another batch is executing.".to_string(),
            ));
        }
        let transaction = TransactionManagementRequest::new(
            TransactionManagementType::Save(name),
            &self.execution_context,
        );
        let mut packet_writer =
            transaction.create_packet_writer(self.transport.as_writer(), None, None);
        transaction.serialize(&mut packet_writer).await?;

        self.consume_transaction_response().await?;

        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn commit_transaction(
        &mut self,
        name: Option<String>,
        create_txn_params: Option<CreateTxnParams>,
    ) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(
                "Cannot commit transaction while another batch is executing.".to_string(),
            ));
        }
        let transaction = TransactionManagementRequest::new(
            TransactionManagementType::Commit {
                name,
                create_txn_params,
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
    pub async fn rollback_transaction(
        &mut self,
        name: Option<String>,
        create_txn_params: Option<CreateTxnParams>,
    ) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(
                "Cannot rollback transaction while another batch is executing.".to_string(),
            ));
        }
        let transaction = TransactionManagementRequest::new(
            TransactionManagementType::Rollback {
                name,
                create_txn_params,
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
    pub async fn get_dtc_address(&mut self) -> TdsResult<()> {
        if self.execution_context.has_open_batch() {
            return Err(UsageError(
                "Cannot get DTC address while another batch is executing.".to_string(),
            ));
        }
        let transaction = TransactionManagementRequest::new(
            TransactionManagementType::GetDtcAddress,
            &self.execution_context,
        );
        let mut packet_writer =
            transaction.create_packet_writer(self.transport.as_writer(), None, None);
        transaction.serialize(&mut packet_writer).await?;

        // GetDtcAddress returns a result set, unlike other transaction commands
        // Set up execution state for result iteration (similar to execute())
        let metadata = self.move_to_column_metadata().await?;
        if metadata.is_none() {
            self.execution_context.set_has_open_batch(false);
        } else {
            self.current_metadata = metadata;
            self.execution_context.set_has_open_batch(true);
        }

        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub(crate) async fn consume_transaction_response(&mut self) -> TdsResult<()> {
        loop {
            let start = Instant::now();
            let token = self
                .transport
                .receive_token(
                    &ParserContext::None(()),
                    self.remaining_request_timeout,
                    self.cancel_handle.as_ref(),
                )
                .await?;
            self.update_remaining_timeout(start);

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
