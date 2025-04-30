use async_trait::async_trait;
use tracing::debug;

use crate::{
    connection::tds_connection::ExecutionContext,
    core::TdsResult,
    datatypes::encoder::GenericEncode,
    message::messages::PacketType,
    read_write::{packet_writer::PacketWriter, reader_writer::NetworkWriter},
    token::tokens::SqlCollation,
};

use super::{
    headers::{TdsHeaders, TransactionDescriptorHeader},
    messages::Request,
    parameters::rpc_parameters::RpcParameter,
};
use crate::message::headers::write_headers;

pub(crate) const PROC_ID_SWITCH: u16 = 0xffff;

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum ProcOptions {
    WithRecompile = 0x01,
    NoMetadata = 0x02,
    ReuseMetadata = 0x04,
}

/// Enum representing the different types of RPCs
/// that can be sent to the server.
pub(crate) enum RpcType {
    Named(String),
    ProcId(RpcProcs),
}

impl<'a> SqlRpc<'a> {
    pub fn new(
        rpc_type: RpcType,
        positional_parameters: Option<Vec<RpcParameter<'a>>>,
        named_parameters: Option<Vec<RpcParameter<'a>>>,
        db_collation: &'a SqlCollation,
        execution_context: &ExecutionContext,
    ) -> Self {
        let transaction_descriptor_header = match execution_context.transaction_descriptor {
            0 => TransactionDescriptorHeader::create_non_transaction_header(),
            transaction_descriptor => TransactionDescriptorHeader::new(
                transaction_descriptor,
                execution_context.outstanding_requests,
            ),
        };
        Self {
            rpc_type,
            headers: Vec::from([transaction_descriptor_header.into()]),
            positional_parameters,
            named_parameters,
            db_collation,
        }
    }

    async fn write_positional_parameters(
        &self,
        packet_writer: &mut PacketWriter<'_>,
    ) -> TdsResult<()> {
        // Implement the logic for writing positional parameters
        // Example: Write a placeholder implementation
        if let Some(positional_parameters) = &self.positional_parameters {
            let encoder = GenericEncode::new();
            for parameter in positional_parameters {
                parameter
                    .serialize(packet_writer, self.db_collation, true, &encoder)
                    .await?;
            }
        } else {
            debug!("Positional parameters are None, skipping serialization.");
        }
        Ok(())
    }

    async fn write_named_parameters(&self, packet_writer: &mut PacketWriter<'_>) -> TdsResult<()> {
        // Implement the logic for writing parameters
        // Example: Write a placeholder implementation
        if let Some(parameters) = &self.named_parameters {
            let encoder = GenericEncode::new();
            for parameter in parameters {
                parameter
                    .serialize(packet_writer, self.db_collation, false, &encoder)
                    .await?;
            }
        }
        Ok(())
    }

    async fn write_proc(&self, packet_writer: &mut PacketWriter<'_>) -> TdsResult<()> {
        match &self.rpc_type {
            RpcType::Named(stored_proc_name) => {
                // Write the procedure name to the packet writer
                packet_writer
                    .write_i16_async((stored_proc_name.len() as u8).into())
                    .await?;
                packet_writer
                    .write_string_unicode_async(stored_proc_name.as_str())
                    .await?;
            }
            RpcType::ProcId(proc) => {
                // Write the procedure ID to the packet writer
                packet_writer.write_u16_async(PROC_ID_SWITCH).await?;
                // Write the int32 value for the procedure ID
                packet_writer
                    .write_i16_async(proc.get_u8_value().into())
                    .await?;
            }
        }
        packet_writer
            .write_i16_async(ProcOptions::NoMetadata as i16)
            .await?;
        Ok(())
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum RpcProcs {
    Cursor = 1,
    CursorOpen = 2,
    CursorPrepare = 3,
    CursorExecute = 4,
    CursorPrepExec = 5,
    CursorUnprepare = 6,
    CursorFetch = 7,
    CursorOption = 8,
    CursorClose = 9,
    ExecuteSql = 10,
    Prepare = 11,
    Execute = 12,
    PrepExec = 13,
    PrepExecRpc = 14,
    Unprepare = 15,
}

impl RpcProcs {
    fn get_u8_value(&self) -> u8 {
        *self as u8
    }
}

pub(crate) struct SqlRpc<'a> {
    pub headers: Vec<TdsHeaders>,
    pub rpc_type: RpcType,
    pub positional_parameters: Option<Vec<RpcParameter<'a>>>,
    pub named_parameters: Option<Vec<RpcParameter<'a>>>,
    pub db_collation: &'a SqlCollation,
}

#[async_trait]
impl<'a> Request<'a> for SqlRpc<'a> {
    fn packet_type(&self) -> PacketType {
        PacketType::RpcRequest
    }

    fn create_packet_writer(&self, writer: &'a mut dyn NetworkWriter) -> PacketWriter<'a> {
        self.packet_type().create_packet_writer(writer)
    }

    async fn serialize(&self, writer: &mut dyn NetworkWriter) -> TdsResult<()> {
        let mut packet_writer = self.create_packet_writer(writer);
        write_headers(&self.headers, &mut packet_writer).await?;
        self.write_proc(&mut packet_writer).await?;
        self.write_positional_parameters(&mut packet_writer).await?;
        self.write_named_parameters(&mut packet_writer).await?;
        packet_writer.finalize().await?;
        Ok(())
    }
}
