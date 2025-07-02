use super::transport::network_transport::NetworkTransport;
use crate::core::{CancelHandle, TdsResult};
use crate::datatypes::column_values::ColumnValues;
use crate::datatypes::sql_string::SqlString;
use crate::datatypes::sqltypes::SqlType;
use crate::error::Error;
use crate::error::Error::UsageError;
use crate::handler::handler_factory::NegotiatedSettings;
use crate::message::attention::AttentionRequest;
use crate::message::batch::SqlBatch;
use crate::message::messages::Request;
use crate::message::parameters::rpc_parameters::{
    build_parameter_list_string, RpcParameter, StatusFlags,
};
use crate::message::rpc::{ProcOptions, RpcProcs, RpcType, SqlRpc};
use crate::message::transaction_management::{
    TransactionManagementRequest, TransactionManagementType,
};
use crate::query::result::BatchResult;
use crate::read_write::packet_reader::{PacketReader, TdsPacketReader};
use crate::read_write::token_stream::{
    GenericTokenParserRegistry, ParserContext, TokenStreamReader,
};
use crate::token::tokens::{
    DoneStatus, EnvChangeContainer, EnvChangeToken, EnvChangeTokenSubType, Tokens,
};
use async_trait::async_trait;
use std::time::{Duration, Instant};
use tracing::{event, info, Level};

pub struct TdsConnection {
    pub(crate) transport: Box<NetworkTransport>,
    pub(crate) negotiated_settings: NegotiatedSettings,
    pub(crate) execution_context: ExecutionContext,
}

const ALREADY_EXECUTING_ERROR: &str = "There is an open BatchResult on the current TdsConnection. It must be closed or fully consumed\
            as a QueryResultTypeStream before executing another operation on this TdsConnection.";

impl<'result> TdsConnection {
    pub(crate) fn new(
        transport: Box<NetworkTransport>,
        negotiated_settings: NegotiatedSettings,
    ) -> Self {
        TdsConnection {
            transport,
            negotiated_settings,
            execution_context: ExecutionContext::new(),
        }
    }

    pub async fn execute(
        &'result mut self,
        sql_command: String,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<BatchResult<'result>> {
        if self.execution_context.has_open_batch {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        };

        let batch = SqlBatch::new(sql_command, &self.execution_context);
        let start = Instant::now();
        batch
            .serialize_and_handle_timeout(self, timeout_sec, cancel_handle)
            .await?;

        let remaining_timeout =
            timeout_sec.map(|t| Duration::from_secs(t as u64) - start.elapsed());
        Ok(BatchResult::new(self, remaining_timeout, cancel_handle))
    }

    // Executes a stored procedure with the given name and parameters.
    // The parameters can be either positional or named.
    pub async fn execute_stored_procedure<'rpc_result>(
        &'result mut self,
        stored_procedure_name: String,
        positional_parameters: Option<&Vec<RpcParameter<'rpc_result>>>,
        named_parameters: Option<&Vec<RpcParameter<'rpc_result>>>,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<BatchResult<'result>> {
        if self.execution_context.has_open_batch {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        };

        let database_collation = self.negotiated_settings.database_collation;

        let rpc = SqlRpc::new(
            RpcType::Named(stored_procedure_name),
            positional_parameters,
            named_parameters,
            &database_collation,
            &self.execution_context,
        );

        let start = Instant::now();
        rpc.serialize_and_handle_timeout(self, timeout_sec, cancel_handle)
            .await?;
        let remaining_timeout =
            timeout_sec.map(|t| Duration::from_secs(t as u64) - start.elapsed());
        Ok(BatchResult::new(self, remaining_timeout, cancel_handle))
    }

    // Executes a stored procedure with the given proc_id and parameters.
    // The parameters can be either positional or named.
    pub async fn execute_sp_executesql<'rpc_result>(
        &'result mut self,
        sql: String,
        named_params: Vec<RpcParameter<'rpc_result>>,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<BatchResult<'result>> {
        if self.execution_context.has_open_batch {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        };

        let database_collation = self.negotiated_settings.database_collation;

        let sql_statement_value = SqlType::NVarcharMax(Some(SqlString::from_utf8_string(sql)));

        // Create the parameter list for sp_execute_sql
        let statement_parameter = RpcParameter::new(None, StatusFlags::NONE, &sql_statement_value);

        // Build the comma separated list of parameters
        let mut params_list_as_string = String::new();

        build_parameter_list_string(&named_params, &mut params_list_as_string);

        let params_as_sql_string =
            SqlType::NVarcharMax(Some(SqlString::from_utf8_string(params_list_as_string)));

        let params_parameter = RpcParameter::new(None, StatusFlags::NONE, &params_as_sql_string);

        // Create the parameter list for positional parameters of sp_execute_sql.
        // These could be named parameters as well, but we want to avoid sending the name
        // to send less data over the wire.
        let positional_parameters_vec = vec![statement_parameter, params_parameter];
        let positional_parameters = Some(&positional_parameters_vec);

        // Build the RPC request.
        let rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::ExecuteSql),
            positional_parameters,
            Some(&named_params),
            &database_collation,
            &self.execution_context,
        );

        let start = Instant::now();
        rpc.serialize_and_handle_timeout(self, timeout_sec, cancel_handle)
            .await?;
        let remaining_timeout =
            timeout_sec.map(|t| Duration::from_secs(t as u64) - start.elapsed());
        Ok(BatchResult::new(self, remaining_timeout, cancel_handle))
    }

    // Prepare a SQL Statement for execution and returns the prepared handle.
    pub async fn execute_sp_prepare<'rpc_result>(
        &'result mut self,
        sql: String,
        named_params: Vec<RpcParameter<'rpc_result>>,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<i32> {
        if self.execution_context.has_open_batch {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        };

        let database_collation = self.negotiated_settings.database_collation;

        let sql_statement_value = SqlType::NVarcharMax(Some(SqlString::from_utf8_string(sql)));

        // Create the parameter list for sp_execute_sql
        let execute_sql_statement_parameter =
            RpcParameter::new(None, StatusFlags::NONE, &sql_statement_value);

        // Build the comma separated list of parameters
        let mut params_list_as_string = String::new();

        build_parameter_list_string(&named_params, &mut params_list_as_string);

        let params_as_sql_string =
            SqlType::NVarcharMax(Some(SqlString::from_utf8_string(params_list_as_string)));

        let params_parameter = RpcParameter::new(None, StatusFlags::NONE, &params_as_sql_string);

        let output_handler_value = SqlType::Int(None);

        let output_handler_parameter = RpcParameter::new(
            None,
            StatusFlags::BY_REF_VALUE, // Output parameter
            &output_handler_value,
        );

        // Create the parameter list for positional parameters of sp_execute_sql.
        // These could be named parameters as well, but we want to avoid sending the name
        // to send less data over the wire.
        let positional_parameters_vec = vec![
            output_handler_parameter,
            params_parameter,
            execute_sql_statement_parameter,
        ];
        let positional_parameters = Some(&positional_parameters_vec);

        // Build the RPC request.
        let rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::Prepare),
            positional_parameters,
            Some(&named_params),
            &database_collation,
            &self.execution_context,
        );

        let start = Instant::now();
        rpc.serialize_and_handle_timeout(self, timeout_sec, cancel_handle)
            .await?;
        let remaining_timeout =
            timeout_sec.map(|t| Duration::from_secs(t as u64) - start.elapsed());
        let mut batch_result = BatchResult::new(self, remaining_timeout, cancel_handle);

        batch_result.close().await?;
        let return_values = batch_result.retrieve_output_params()?;

        // We need to get the return value, and then extract the handle from it.
        match return_values {
            Some(return_values) => {
                if return_values.len() == 1 {
                    let returned_parameter = return_values.first().unwrap();
                    if let ColumnValues::Int(handle) = &returned_parameter.value {
                        Ok(*handle)
                    } else {
                        Err(Error::ProtocolError(
                            "Unexpected an integer value".to_string(),
                        ))
                    }
                } else {
                    Err(Error::ProtocolError(
                        "Unexpected empty output parametes".to_string(),
                    ))
                }
            }
            None => Err(Error::ProtocolError(
                "Unexpected empty output parametes".to_string(),
            )),
        }
    }

    pub async fn execute_sp_unprepare(
        &'result mut self,
        handle: i32,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<()> {
        if self.execution_context.has_open_batch {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        };

        let database_collation = self.negotiated_settings.database_collation;

        let handle_value = SqlType::Int(Some(handle));
        let handle_parameter = RpcParameter::new(
            None,
            StatusFlags::NONE, // Output parameter
            &handle_value,
        );

        // Create the parameter list for positional parameters of sp_execute_sql.
        // These could be named parameters as well, but we want to avoid sending the name
        // to send less data over the wire.
        let positional_parameters_vec = vec![handle_parameter];
        let positional_parameters = Some(&positional_parameters_vec);

        // Build the RPC request.
        let rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::Unprepare),
            positional_parameters,
            None,
            &database_collation,
            &self.execution_context,
        );

        let start = Instant::now();
        rpc.serialize_and_handle_timeout(self, timeout_sec, cancel_handle)
            .await?;

        // Drain the result set. A successful unprepare will not return any results.
        let remaining_timeout =
            timeout_sec.map(|t| Duration::from_secs(t as u64) - start.elapsed());
        let mut result = BatchResult::new(self, remaining_timeout, cancel_handle);
        result.close().await?;
        Ok(())
    }

    // Executes sp_prepexec which will prepare the statement for execution, return a result set
    // as well as a prepared handle.
    pub async fn execute_sp_prepexec<'rpc_result>(
        &'result mut self,
        sql: String,
        named_params: &Vec<RpcParameter<'rpc_result>>,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<BatchResult<'result>> {
        if self.execution_context.has_open_batch {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        };

        let database_collation = self.negotiated_settings.database_collation;

        let sql_statement_value = SqlType::NVarcharMax(Some(SqlString::from_utf8_string(sql)));

        // Create the parameter list for sp_execute_sql
        let statement_parameter = RpcParameter::new(None, StatusFlags::NONE, &sql_statement_value);

        // Build the comma separated list of parameters
        let mut params_list_as_string = String::new();

        build_parameter_list_string(named_params, &mut params_list_as_string);

        let params_as_sql_string =
            SqlType::NVarcharMax(Some(SqlString::from_utf8_string(params_list_as_string)));

        let params_parameter = RpcParameter::new(None, StatusFlags::NONE, &params_as_sql_string);

        let handle_value = SqlType::Int(None);

        let handle_parameter = RpcParameter::new(None, StatusFlags::BY_REF_VALUE, &handle_value);

        // Create the parameter list for positional parameters of sp_prepareexec.
        // These could be named parameters as well, but we want to avoid sending the name
        // to send less data over the wire.
        let positional_parameters_list =
            vec![handle_parameter, params_parameter, statement_parameter];
        let positional_parameters = Some(&positional_parameters_list);

        // Build the RPC request.
        let rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::PrepExec),
            positional_parameters,
            Some(named_params),
            &database_collation,
            &self.execution_context,
        );

        let start = Instant::now();
        rpc.serialize_and_handle_timeout(self, timeout_sec, cancel_handle)
            .await?;
        let remaining_timeout =
            timeout_sec.map(|t| Duration::from_secs(t as u64) - start.elapsed());
        Ok(BatchResult::new(self, remaining_timeout, cancel_handle))
    }

    pub async fn execute_sp_execute<'rpc_result>(
        &'result mut self,
        handle: i32,
        positional_parameters: Option<Vec<RpcParameter<'rpc_result>>>,
        named_parameters: Option<&Vec<RpcParameter<'rpc_result>>>,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<BatchResult<'result>> {
        if self.execution_context.has_open_batch {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        };

        let database_collation = self.negotiated_settings.database_collation;

        let handle_value = SqlType::Int(Some(handle));
        let handle_parameter = RpcParameter::new(
            None,
            StatusFlags::NONE, // Output parameter
            &handle_value,
        );

        // Create the parameter list for positional parameters of sp_execute_sql.
        // These could be named parameters as well, but we want to avoid sending the name
        // to send less data over the wire.
        let mut all_positional_parameters = vec![handle_parameter];

        if let Some(mut params) = positional_parameters {
            all_positional_parameters.append(&mut params);
        }
        let all_positional_parameters = Some(&all_positional_parameters);

        // Build the RPC request.
        let mut rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::Execute),
            all_positional_parameters,
            named_parameters,
            &database_collation,
            &self.execution_context,
        );

        // TODO: This needs to be removed after we enhance the metadata propagation in case of null metadata.
        // Right now, if NoMetadata is set for the options, the the SQL server doesnt return metadata.
        // It is expected that the client caches the metadata and reuses MD to read the row tokens.
        // ReuseMetadata will cause the server to return the metadata with sp_execute. This means that
        // more information is being sent over the network.
        rpc.set_proc_options(ProcOptions::ReuseMetadata);
        let start = Instant::now();
        rpc.serialize_and_handle_timeout(self, timeout_sec, cancel_handle)
            .await?;

        // Drain the result set. A successful unprepare will not return any results.
        let remaining_timeout =
            timeout_sec.map(|t| Duration::from_secs(t as u64) - start.elapsed());
        let result = BatchResult::new(self, remaining_timeout, cancel_handle);

        Ok(result)
    }

    pub async fn send_transaction(
        &'result mut self,
        transaction_params: TransactionManagementType,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<BatchResult<'result>> {
        if self.execution_context.has_open_batch {
            return Err(UsageError(ALREADY_EXECUTING_ERROR.to_string()));
        };

        let transaction =
            TransactionManagementRequest::new(transaction_params, &self.execution_context);

        let start = Instant::now();
        transaction
            .serialize_and_handle_timeout(self, timeout_sec, cancel_handle)
            .await?;

        let remaining_timeout =
            timeout_sec.map(|t| Duration::from_secs(t as u64) - start.elapsed());
        Ok(BatchResult::new(self, remaining_timeout, cancel_handle))
    }

    pub(crate) async fn send_attention(
        &'result mut self,
        timeout_sec: Option<u32>,
    ) -> TdsResult<()> {
        let attention = AttentionRequest::new();
        attention
            .serialize_and_handle_timeout(self, timeout_sec, None)
            .await?;

        self.drain_until_done_status(DoneStatus::ATTN).await;
        self.execution_context.has_open_batch = false;
        Ok(())
    }

    pub(crate) async fn drain_until_done_status(&'result mut self, search_status: DoneStatus) {
        let packet_reader = PacketReader::new(self.transport.as_mut());
        let mut token_stream_reader = TokenStreamReader::new(
            packet_reader,
            Box::new(GenericTokenParserRegistry::default()),
        );
        let parser_context = ParserContext::None(());

        // Drain the stream until we receive a Done with the Attention bit set.
        while let Ok(token) = token_stream_reader
            .receive_token(&parser_context, None, None)
            .await
        {
            match token {
                Tokens::Done(t1) => {
                    info!(?t1);
                    if t1.status.contains(search_status) {
                        break;
                    }
                }
                _ => {
                    info!(?token);
                }
            }
        }
        self.execution_context.has_open_batch = false;
        self.execution_context.has_open_result_set = false;
    }
}

#[async_trait]
impl TdsPacketReader for TdsConnection {
    async fn read_byte(&mut self) -> TdsResult<u8> {
        todo!();
    }
    async fn read_int16_big_endian(&mut self) -> TdsResult<i16> {
        todo!();
    }
    async fn read_int32_big_endian(&mut self) -> TdsResult<i32> {
        todo!();
    }
    async fn read_int64_big_endian(&mut self) -> TdsResult<i64> {
        todo!();
    }
    async fn read_uint40(&mut self) -> TdsResult<u64> {
        todo!();
    }

    async fn read_float32(&mut self) -> TdsResult<f32> {
        todo!();
    }
    async fn read_float64(&mut self) -> TdsResult<f64> {
        todo!();
    }
    async fn read_int16(&mut self) -> TdsResult<i16> {
        todo!();
    }
    async fn read_uint16(&mut self) -> TdsResult<u16> {
        todo!();
    }
    async fn read_int24(&mut self) -> TdsResult<i32> {
        todo!();
    }
    async fn read_uint24(&mut self) -> TdsResult<u32> {
        todo!();
    }
    async fn read_int32(&mut self) -> TdsResult<i32> {
        todo!();
    }
    async fn read_uint32(&mut self) -> TdsResult<u32> {
        todo!();
    }
    async fn read_int64(&mut self) -> TdsResult<i64> {
        todo!();
    }
    async fn read_uint64(&mut self) -> TdsResult<u64> {
        todo!();
    }

    async fn read_bytes(&mut self, _buffer: &mut [u8]) -> TdsResult<usize> {
        todo!();
    }
    async fn read_u8_varbyte(&mut self) -> TdsResult<Vec<u8>> {
        todo!();
    }
    async fn read_u16_varbyte(&mut self) -> TdsResult<Vec<u8>> {
        todo!();
    }
    async fn read_varchar_u16_length(&mut self) -> TdsResult<Option<String>> {
        todo!();
    }
    async fn read_varchar_u8_length(&mut self) -> TdsResult<String> {
        todo!();
    }
    async fn read_varchar_byte_len(&mut self) -> TdsResult<String> {
        todo!();
    }
    async fn read_unicode(&mut self, _string_length: usize) -> TdsResult<String> {
        todo!();
    }
    async fn read_unicode_with_byte_length(&mut self, _byte_length: usize) -> TdsResult<String> {
        todo!();
    }
    async fn skip_bytes(&mut self, _skip_count: usize) -> TdsResult<()> {
        todo!();
    }
    async fn cancel_read_stream(&mut self) -> TdsResult<()> {
        todo!();
    }
}

/// Represents the execution context of a TDS connection.
/// It holds information about the current transaction,
/// outstanding requests, and whether there are open batches or result sets.
/// This context is used to manage the state of the query execution on the connection.
pub(crate) struct ExecutionContext {
    pub transaction_descriptor: u64,
    pub outstanding_requests: u32,
    pub has_open_batch: bool,
    pub has_open_result_set: bool,
}

impl ExecutionContext {
    pub(crate) fn new() -> Self {
        Self {
            transaction_descriptor: 0,
            outstanding_requests: 1,
            has_open_batch: false,
            has_open_result_set: false,
        }
    }

    pub(crate) fn capture_change_property(
        &mut self,
        change_token: &EnvChangeToken,
    ) -> TdsResult<()> {
        let sub_type = change_token.sub_type;

        match change_token.change_type {
            EnvChangeContainer::UInt64(u64_change) => match sub_type {
                EnvChangeTokenSubType::BeginTransaction
                | EnvChangeTokenSubType::CommitTransaction
                | EnvChangeTokenSubType::RollbackTransaction
                | EnvChangeTokenSubType::EnlistDtcTransaction
                | EnvChangeTokenSubType::DefectTransaction => {
                    self.transaction_descriptor = *u64_change.new_value();
                    Ok(())
                }
                _ => {
                    event!(
                        Level::ERROR,
                        "Unknown change property type: {:?}",
                        change_token.change_type
                    );
                    Err(crate::error::Error::ProtocolError(
                        "Unknown change property type".to_string(),
                    ))
                }
            },
            _ => {
                event!(
                    Level::ERROR,
                    "Unknown change property type: {:?}",
                    change_token.change_type
                );
                Err(crate::error::Error::ProtocolError(
                    "Unknown change property type".to_string(),
                ))
            }
        }
    }
}
