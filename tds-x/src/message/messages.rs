use std::io::Error;

use crate::{
    read_write::{
        packet_writer::PacketWriter,
        reader_writer::{NetworkReader, NetworkWriter},
    },
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

/// Represents the status flags for a packet.
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

#[async_trait(?Send)]
pub trait Request<'a> {
    fn packet_type(&self) -> PacketType;
    fn create_packet_writer(&self, writer: &'a mut dyn NetworkWriter) -> PacketWriter<'a>;
    async fn serialize(&self, _transport: &mut dyn NetworkWriter) -> Result<(), Error>;
}

#[async_trait(?Send)]
pub trait TypedResponse<T> {
    async fn deserialize(&self, reader: &mut dyn NetworkReader) -> T;
}

pub(crate) struct TdsError {
    error_token: ErrorToken,
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
