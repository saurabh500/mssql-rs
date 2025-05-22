use super::transport::network_transport::NetworkTransport;
use crate::core::{CancelHandle, TdsResult};
use crate::datatypes::decoder::ColumnValues;
use crate::datatypes::sql_string::SqlString;
use crate::datatypes::sqldatatypes::TdsDataType;
use crate::error::Error;
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
use crate::read_write::packet_reader::PacketReader;
use crate::read_write::token_stream::{
    GenericTokenParserRegistry, ParserContext, TokenStreamReader,
};
use crate::token::tokens::{
    DoneStatus, EnvChangeContainer, EnvChangeToken, EnvChangeTokenSubType, Tokens,
};
use std::time::{Duration, Instant};
use tracing::{event, info, Level};

pub struct TdsConnection<'a> {
    pub(crate) transport: Box<NetworkTransport<'a>>,
    pub(crate) negotiated_settings: NegotiatedSettings,
    pub(crate) execution_context: ExecutionContext,
}

impl<'connection, 'result> TdsConnection<'connection> {
    pub async fn execute(
        &'result mut self,
        sql_command: String,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<BatchResult<'result>>
    where
        'connection: 'result,
    {
        let batch = SqlBatch::new(sql_command, &self.execution_context);
        let start = Instant::now();
        batch
            .serialize_and_handle_timeout(self, timeout_sec, cancel_handle)
            .await?;

        let time_limit = match timeout_sec {
            Some(t) => start.checked_add(Duration::from_secs(t as u64)),
            None => None,
        };
        Ok(BatchResult::new(self, time_limit, cancel_handle))
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
        let time_limit = match timeout_sec {
            Some(t) => start.checked_add(Duration::from_secs(t as u64)),
            None => None,
        };
        Ok(BatchResult::new(self, time_limit, cancel_handle))
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
        let database_collation = self.negotiated_settings.database_collation;

        let sql_statement_value = ColumnValues::String(SqlString::from_utf8_string(sql));

        // Create the parameter list for sp_execute_sql
        let statement_parameter = RpcParameter::new(
            None,
            StatusFlags::NONE,
            &TdsDataType::NVarChar,
            false,
            &sql_statement_value,
        );

        // Build the comma separated list of parameters
        let mut params_list_as_string = String::new();

        build_parameter_list_string(&named_params, &mut params_list_as_string);

        let params_as_sql_string =
            ColumnValues::String(SqlString::from_utf8_string(params_list_as_string));

        let params_parameter = RpcParameter::new(
            None,
            StatusFlags::NONE,
            &TdsDataType::NVarChar,
            false,
            &params_as_sql_string,
        );

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
        let time_limit = match timeout_sec {
            Some(t) => start.checked_add(Duration::from_secs(t as u64)),
            None => None,
        };
        Ok(BatchResult::new(self, time_limit, cancel_handle))
    }

    // Prepare a SQL Statement for execution and returns the prepared handle.
    pub async fn execute_sp_prepare<'rpc_result>(
        &'result mut self,
        sql: String,
        named_params: Vec<RpcParameter<'rpc_result>>,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<i32> {
        let database_collation = self.negotiated_settings.database_collation;

        let sql_statement_value = ColumnValues::String(SqlString::from_utf8_string(sql));

        // Create the parameter list for sp_execute_sql
        let execute_sql_statement_parameter = RpcParameter::new(
            None,
            StatusFlags::NONE,
            &TdsDataType::NVarChar,
            false,
            &sql_statement_value,
        );

        // Build the comma separated list of parameters
        let mut params_list_as_string = String::new();

        build_parameter_list_string(&named_params, &mut params_list_as_string);

        let params_as_sql_string =
            ColumnValues::String(SqlString::from_utf8_string(params_list_as_string));

        let params_parameter = RpcParameter::new(
            None,
            StatusFlags::NONE,
            &TdsDataType::NVarChar,
            false,
            &params_as_sql_string,
        );

        let output_handler_parameter = RpcParameter::new(
            None,
            StatusFlags::BY_REF_VALUE, // Output parameter
            &TdsDataType::Int4,
            false,
            &ColumnValues::Null, // This is an output parameter. Set to null.
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
        let time_limit = match timeout_sec {
            Some(t) => start.checked_add(Duration::from_secs(t as u64)),
            None => None,
        };
        let mut batch_result = BatchResult::new(self, time_limit, cancel_handle);

        let return_values = batch_result.close().await?;

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
        let database_collation = self.negotiated_settings.database_collation;

        let handle_value = ColumnValues::Int(handle);
        let handle_parameter = RpcParameter::new(
            None,
            StatusFlags::NONE, // Output parameter
            &TdsDataType::Int4,
            false,
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
        let time_limit = match timeout_sec {
            Some(t) => start.checked_add(Duration::from_secs(t as u64)),
            None => None,
        };
        let mut result = BatchResult::new(self, time_limit, cancel_handle);
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
        let database_collation = self.negotiated_settings.database_collation;

        let sql_statement_value = ColumnValues::String(SqlString::from_utf8_string(sql));

        // Create the parameter list for sp_execute_sql
        let statement_parameter = RpcParameter::new(
            None,
            StatusFlags::NONE,
            &TdsDataType::NVarChar,
            false,
            &sql_statement_value,
        );

        // Build the comma separated list of parameters
        let mut params_list_as_string = String::new();

        build_parameter_list_string(named_params, &mut params_list_as_string);

        let params_as_sql_string =
            ColumnValues::String(SqlString::from_utf8_string(params_list_as_string));

        let params_parameter = RpcParameter::new(
            None,
            StatusFlags::NONE,
            &TdsDataType::NVarChar,
            false,
            &params_as_sql_string,
        );

        let handle_parameter = RpcParameter::new(
            None,
            StatusFlags::BY_REF_VALUE,
            &TdsDataType::Int4,
            false,
            &ColumnValues::Null,
        );

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

        let time_limit = match timeout_sec {
            Some(t) => start.checked_add(Duration::from_secs(t as u64)),
            None => None,
        };
        Ok(BatchResult::new(self, time_limit, cancel_handle))
    }

    pub async fn execute_sp_execute<'rpc_result>(
        &'result mut self,
        handle: i32,
        positional_parameters: Option<Vec<RpcParameter<'rpc_result>>>,
        named_parameters: Option<&Vec<RpcParameter<'rpc_result>>>,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<BatchResult<'result>> {
        let database_collation = self.negotiated_settings.database_collation;

        let handle_value = ColumnValues::Int(handle);
        let handle_parameter = RpcParameter::new(
            None,
            StatusFlags::NONE, // Output parameter
            &TdsDataType::Int4,
            false,
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
        let time_limit = match timeout_sec {
            Some(t) => start.checked_add(Duration::from_secs(t as u64)),
            None => None,
        };
        let result = BatchResult::new(self, time_limit, cancel_handle);

        Ok(result)
    }

    pub async fn send_transaction(
        &'result mut self,
        transaction_params: TransactionManagementType,
        timeout_sec: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<BatchResult<'result>> {
        let transaction =
            TransactionManagementRequest::new(transaction_params, &self.execution_context);

        let start = Instant::now();
        transaction
            .serialize_and_handle_timeout(self, timeout_sec, cancel_handle)
            .await?;

        let time_limit = match timeout_sec {
            Some(t) => start.checked_add(Duration::from_secs(t as u64)),
            None => None,
        };

        Ok(BatchResult::new(self, time_limit, cancel_handle))
    }

    pub async fn cancel(&'result mut self) -> TdsResult<()> {
        Ok(())
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
    }
}

pub(crate) struct ExecutionContext {
    pub transaction_descriptor: u64,
    pub outstanding_requests: u32,
}

impl ExecutionContext {
    pub(crate) fn new() -> Self {
        Self {
            transaction_descriptor: 0,
            outstanding_requests: 1,
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
