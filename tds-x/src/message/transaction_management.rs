use crate::connection::tds_connection::ExecutionContext;
use crate::core::TdsResult;
use crate::message::headers::{write_headers, TdsHeaders, TransactionDescriptorHeader};
use crate::message::messages::PacketType::TransactionManager;
use crate::message::messages::{PacketType, Request};
use crate::read_write::packet_writer::{PacketWriter, TdsPacketWriter};
use async_trait::async_trait;
use std::io::Error;

pub struct CreateTxnParams {
    pub level: TransactionIsolationLevel,
    pub name: Option<String>,
}

impl CreateTxnParams {
    async fn serialize(&self, writer: &mut PacketWriter<'_>) -> TdsResult<()> {
        writer.write_byte_async(self.level.clone() as u8).await?;
        match self.name.as_ref() {
            Some(name) => {
                writer.write_byte_async((name.len() * 2) as u8).await?;
                writer.write_string_unicode_async(name).await?;
                Ok(())
            }
            None => {
                writer.write_byte_async(0).await?;
                Ok(())
            }
        }
    }
}

#[repr(u16)]
pub enum TransactionManagementType {
    GetDtcAddress = 0,
    Propagate(String) = 1,
    Begin(CreateTxnParams) = 5,
    Promote = 6,
    Commit {
        name: Option<String>,
        create_txn_params: Option<CreateTxnParams>,
    } = 7,
    Rollback {
        name: Option<String>,
        create_txn_params: Option<CreateTxnParams>,
    } = 8,
    Save(String) = 9,
}

#[repr(u8)]
#[derive(Debug, Clone)]
pub enum TransactionIsolationLevel {
    NoChange = 0x00,
    ReadUncommitted = 0x01,
    ReadCommitted = 0x02,
    RepeatableRead = 0x03,
    Serializable = 0x04,
    Snapshot = 0x05,
}

pub(crate) struct TransactionManagementRequest {
    pub(crate) headers: Vec<TdsHeaders>,
    pub(crate) transaction_params: TransactionManagementType,
}

impl TransactionManagementRequest {
    pub fn new(
        transaction_params: TransactionManagementType,
        execution_context: &ExecutionContext,
    ) -> Self {
        let transaction_descriptor_header = TransactionDescriptorHeader::new(
            execution_context.transaction_descriptor,
            execution_context.outstanding_requests,
        );
        Self {
            headers: Vec::from([transaction_descriptor_header.into()]),
            transaction_params,
        }
    }
}

impl From<&TransactionManagementType> for u16 {
    fn from(value: &TransactionManagementType) -> Self {
        use super::transaction_management::TransactionManagementType::{
            Begin, Commit, GetDtcAddress, Promote, Propagate, Rollback, Save,
        };

        match value {
            GetDtcAddress => 0,
            Propagate(..) => 1,
            Begin(..) => 5,
            Promote => 6,
            Commit { .. } => 7,
            Rollback { .. } => 8,
            Save(..) => 9,
        }
    }
}

#[async_trait]
impl Request for TransactionManagementRequest {
    fn packet_type(&self) -> PacketType {
        TransactionManager
    }

    async fn serialize<'a, 'b>(&'a self, packet_writer: &'a mut PacketWriter<'b>) -> TdsResult<()>
    where
        'b: 'a,
    {
        write_headers(&self.headers, packet_writer).await?;
        packet_writer
            .write_u16_async(u16::from(&self.transaction_params))
            .await?;
        match &self.transaction_params {
            TransactionManagementType::GetDtcAddress => {
                packet_writer.write_u16_async(0).await?;
            }
            TransactionManagementType::Propagate(payload) => {
                packet_writer
                    .write_u16_async((payload.len() * 2) as u16)
                    .await?;
                packet_writer.write_string_unicode_async(payload).await?;
            }
            TransactionManagementType::Begin(payload) => {
                payload.serialize(packet_writer).await?;
            }
            TransactionManagementType::Promote => (),
            TransactionManagementType::Commit {
                name,
                create_txn_params: new_transaction_metadata,
            }
            | TransactionManagementType::Rollback {
                name,
                create_txn_params: new_transaction_metadata,
            } => {
                if let Some(transaction_name) = &name {
                    packet_writer
                        .write_byte_async((transaction_name.len() * 2) as u8)
                        .await?;
                    packet_writer
                        .write_string_unicode_async(transaction_name)
                        .await?;
                } else {
                    packet_writer.write_byte_async(0).await?;
                }
                match new_transaction_metadata {
                    Some(new_transaction_metadata) => {
                        let flag: u8 = 1u8 << 7;
                        packet_writer.write_byte_async(flag).await?;
                        new_transaction_metadata.serialize(packet_writer).await?;
                    }
                    None => {
                        packet_writer.write_byte_async(0).await?;
                    }
                }
            }
            TransactionManagementType::Save(payload) => {
                // Savepoint must be non-empty.
                if payload.is_empty() {
                    Err(crate::error::Error::from(Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Savepoint name must be non-empty.",
                    )))?
                }
                packet_writer
                    .write_byte_async((payload.len() * 2) as u8)
                    .await?;
                packet_writer.write_string_unicode_async(payload).await?;
            }
        }
        packet_writer.finalize().await?;
        Ok(())
    }
}
