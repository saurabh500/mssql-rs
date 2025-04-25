use super::transport::network_transport::NetworkTransport;
use crate::core::TdsResult;
use crate::handler::handler_factory::NegotiatedSettings;
use crate::message::batch::SqlBatch;
use crate::message::messages::Request;
use crate::message::parameters::rpc_parameters::RpcParameter;
use crate::message::rpc::{RpcType, SqlRpc};
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
        // let response = SqlQueryResponse::new(tds_connection);

        Ok(BatchResult::new(self))
    }

    pub async fn execute_stored_procedure<'b>(
        &'result mut self,
        sql: String,
        positional_parameters: Option<Vec<RpcParameter<'b>>>,
        named_parameters: Option<Vec<RpcParameter<'b>>>,
    ) -> TdsResult<BatchResult<'result>> {
        let database_collation = self.negotiated_settings.database_collation;

        let rpc = SqlRpc::new(
            RpcType::Named { name: sql },
            positional_parameters,
            named_parameters,
            &database_collation,
            &self.execution_context,
        );

        rpc.serialize(self.transport.as_mut()).await?;
        Ok(BatchResult::new(self))
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
