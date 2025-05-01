use super::transport::network_transport::NetworkTransport;
use crate::core::TdsResult;
use crate::datatypes::decoder::ColumnValues;
use crate::datatypes::sql_string::SqlString;
use crate::datatypes::sqldatatypes::TdsDataType;
use crate::error::Error;
use crate::handler::handler_factory::NegotiatedSettings;
use crate::message::batch::SqlBatch;
use crate::message::messages::Request;
use crate::message::parameters::rpc_parameters::{
    build_parameter_list_string, RpcParameter, StatusFlags,
};
use crate::message::rpc::{RpcProcs, RpcType, SqlRpc};
use crate::message::transaction_management::{
    TransactionManagementRequest, TransactionManagementType,
};
use crate::query::result::BatchResult;
use crate::token::tokens::{EnvChangeContainer, EnvChangeToken, EnvChangeTokenSubType};
use tracing::{event, Level};

pub struct TdsConnection<'a> {
    pub(crate) transport: Box<NetworkTransport<'a>>,
    pub(crate) negotiated_settings: NegotiatedSettings,
    pub(crate) execution_context: ExecutionContext,
}

impl<'connection, 'result> TdsConnection<'connection> {
    pub async fn execute(&'result mut self, sql_command: String) -> TdsResult<BatchResult<'result>>
    where
        'connection: 'result,
    {
        let batch = SqlBatch::new(sql_command, &self.execution_context);

        batch.serialize(self.transport.as_mut()).await?;

        Ok(BatchResult::new(self))
    }

    // Executes a stored procedure with the given name and parameters.
    // The parameters can be either positional or named.
    pub async fn execute_stored_procedure<'rpc_result>(
        &'result mut self,
        stored_procedure_name: String,
        positional_parameters: Option<Vec<RpcParameter<'rpc_result>>>,
        named_parameters: Option<Vec<RpcParameter<'rpc_result>>>,
    ) -> TdsResult<BatchResult<'result>> {
        let database_collation = self.negotiated_settings.database_collation;

        let rpc = SqlRpc::new(
            RpcType::Named(stored_procedure_name),
            positional_parameters,
            named_parameters,
            &database_collation,
            &self.execution_context,
        );

        rpc.serialize(self.transport.as_mut()).await?;
        Ok(BatchResult::new(self))
    }

    // Executes a stored procedure with the given proc_id and parameters.
    // The parameters can be either positional or named.
    pub async fn execute_sql_rpc<'rpc_result>(
        &'result mut self,
        sql: String,
        named_params: Vec<RpcParameter<'rpc_result>>,
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
        let positional_parameters = Some(vec![statement_parameter, params_parameter]);

        // Build the RPC request.
        let rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::ExecuteSql),
            positional_parameters,
            Some(named_params),
            &database_collation,
            &self.execution_context,
        );

        rpc.serialize(self.transport.as_mut()).await?;
        Ok(BatchResult::new(self))
    }

    // Prepare a SQL Statement for execution and returns the prepared handle.
    pub async fn execute_prepare<'rpc_result>(
        &'result mut self,
        sql: String,
        named_params: Vec<RpcParameter<'rpc_result>>,
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
        let positional_parameters = Some(vec![
            output_handler_parameter,
            params_parameter,
            execute_sql_statement_parameter,
        ]);

        // Build the RPC request.
        let rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::Prepare),
            positional_parameters,
            Some(named_params),
            &database_collation,
            &self.execution_context,
        );

        rpc.serialize(self.transport.as_mut()).await?;
        let mut batch_result = BatchResult::new(self);

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

    pub async fn execute_unprepare(&'result mut self, handle: i32) -> TdsResult<()> {
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
        let positional_parameters = Some(vec![handle_parameter]);

        // Build the RPC request.
        let rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::Prepare),
            positional_parameters,
            None,
            &database_collation,
            &self.execution_context,
        );

        rpc.serialize(self.transport.as_mut()).await?;

        // Drain the result set. A successful unprepare will not return any results.
        let mut result = BatchResult::new(self);
        result.close().await?;
        Ok(())
    }

    pub async fn transaction(
        &'result mut self,
        transaction_params: TransactionManagementType,
    ) -> TdsResult<BatchResult<'result>> {
        let transaction =
            TransactionManagementRequest::new(transaction_params, &self.execution_context);
        transaction.serialize(self.transport.as_mut()).await?;

        Ok(BatchResult::new(self))
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
