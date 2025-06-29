use crate::connection::tds_connection::TdsConnection;
use crate::core::{CancelHandle, NegotiatedEncryptionSetting, TdsResult};
use crate::error::Error::{OperationCancelledError, TimeoutError};
use crate::read_write::packet_writer::MessageSendState;
use crate::read_write::packet_writer::MessageSendState::NotStarted;
use crate::token::tokens::DoneStatus;
use crate::{
    read_write::{packet_writer::PacketWriter, reader_writer::NetworkWriter},
    token::tokens::ErrorToken,
};
use async_trait::async_trait;

#[derive(Copy, Clone)]
pub enum PacketType {
    Unknown = 0x00,
    SqlBatch = 0x01,
    RpcRequest = 0x03,
    TabularResult = 0x04,
    Attention = 0x06,
    BulkLoad = 0x07,
    FedAuthToken = 0x08,
    TransactionManager = 0x0E,
    Login7 = 0x10,
    SSPI = 0x11,
    PreLogin = 0x12,
}

impl<'a, 'b> PacketType
where
    'a: 'b,
{
    pub(crate) fn create_packet_writer(
        &self,
        transport: &'a mut dyn NetworkWriter,
        timeout: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> PacketWriter<'a> {
        PacketWriter::new(*self, transport, timeout, cancel_handle)
    }

    pub(crate) async fn first_packet_callback(
        &self,
        writer: &'b mut dyn NetworkWriter,
    ) -> TdsResult<()> {
        match self {
            PacketType::Login7 => {
                if writer.get_encryption_setting() == NegotiatedEncryptionSetting::LoginOnly {
                    // Only the first packet should be encrypted. Turn off encryption after the first packet.
                    writer.disable_ssl().await
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }
}

/// Represents the status flags for a packet.
#[repr(u8)]
pub(crate) enum PacketStatusFlags {
    /// Normal Packet.
    Normal = 0x00,

    /// End of Message. The last packet in the message.
    Eom = 0x01,

    /// Packet/Message to be ignored.
    Ignore = 0x02,

    /// Reset connection.
    ResetConnection = 0x08,

    /// Reset connection but keep transaction state.
    ResetConnectionSkipTran = 0x10,
}

#[async_trait]
pub(crate) trait Request {
    fn packet_type(&self) -> PacketType;

    fn create_packet_writer<'a>(
        &self,
        writer: &'a mut dyn NetworkWriter,
        timeout: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> PacketWriter<'a> {
        self.packet_type()
            .create_packet_writer(writer, timeout, cancel_handle)
    }

    async fn serialize<'a, 'b>(&'a self, writer: &'a mut PacketWriter<'b>) -> TdsResult<()>
    where
        'b: 'a;

    async fn serialize_and_handle_timeout(
        &self,
        connection: &mut TdsConnection,
        timeout: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<()> {
        let mut message_state = None;
        let serialize_result = {
            let mut packet_writer =
                self.create_packet_writer(connection.transport.as_mut(), timeout, cancel_handle);
            let result = self.serialize(&mut packet_writer).await;
            match &result {
                Ok(_) => {}
                Err(err) => {
                    match err {
                        OperationCancelledError(_) | TimeoutError(_) => {
                            // Handle the timeout differently depending on the state of the PacketWriter.
                            message_state = Some(packet_writer.get_message_state());
                            match message_state.as_ref().unwrap() {
                                NotStarted | MessageSendState::Complete => {}
                                // No-op. For completed requests, handle during batch iteration.
                                MessageSendState::Partial => {
                                    packet_writer.cancel_current_message().await?;
                                    // Note - more cleanup needed after relinquishing the connection.
                                }
                            };
                        }
                        _ => {}
                    }
                }
            }
            result
        };

        match message_state {
            None | Some(NotStarted) => {} // No other work needed.
            Some(MessageSendState::Partial) => {
                // Drain response until we get a Done with Error to show the message was cancelled.
                connection.drain_until_done_status(DoneStatus::ERROR).await;
            }
            Some(MessageSendState::Complete) => {
                // Send the attention request.
                connection.send_attention(None).await?
            }
        };
        serialize_result
    }
}

pub(crate) struct TdsError {
    pub(crate) error_token: ErrorToken,
}

impl TdsError {
    pub fn new(error_token: ErrorToken) -> Self {
        TdsError { error_token }
    }

    pub fn get_message(&self) -> String {
        self.error_token.message.clone()
    }
}

pub struct TdsInfo {}

pub struct TokenResponse {}

impl TokenResponse {
    // TODO:
}
