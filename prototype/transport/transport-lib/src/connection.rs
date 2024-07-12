pub mod builder;
mod transport;
mod packet;
mod token;

use super::{Result,EncryptionLevel};
use transport::Transport;
use packet::{PacketHeader, PacketType, Packet, HEADER_BYTES};

use tracing::{event, Level};
use std::io::Write;
use bytes::{Buf, BufMut, BytesMut};

enum LoginState {
    None,
    PreLogin,
    Login,
    LoginAck,
}

pub(crate) struct Connection
{
    transport: Transport,
    server_encryption: Option<EncryptionLevel>,
    fed_auth_required: bool,
    packet_id: u8,
    pending_handshake: bool,
    login_state: LoginState,
}

impl Connection {
    pub(crate) fn new() -> Self {
        Self {
            transport: Transport::None,
            packet_id: 0,
            pending_handshake: true,
            server_encryption: None,
            fed_auth_required: false,
            login_state: LoginState::None,
        }
    }

    fn encryption_required(&self) -> bool {
        match  self.server_encryption.unwrap_or(EncryptionLevel::Off) {
            EncryptionLevel::Off => false,
            EncryptionLevel::On => true,
            EncryptionLevel::NotSupported => false,
            EncryptionLevel::Required => true,
        }
    }

    fn next_packet_id(&mut self) -> u8 {
        let id = self.packet_id;
        self.packet_id = self.packet_id.wrapping_add(1);
        id
    }

    fn send<E>(&mut self, ty: PacketType, item: E) -> Result<()>
        where E: Encode<BytesMut>
    {
        let packet_id = self.next_packet_id();
        let header = PacketHeader::new(ty, packet_id);
        let mut data = BytesMut::new();
        item.encode(&mut data)?;
        let packet = Packet::new(header, data);
        let mut payload = BytesMut::new();
        packet.encode(&mut payload)?;
        event!(
            Level::DEBUG,
            "Sending a packet {} ({} bytes)",
            packet_id,
            payload.len() + HEADER_BYTES,
        );
        self.transport.write(&payload)?;
        self.transport.flush()?;
        Ok(())
    }

    fn collect_packet(&mut self) -> Result<Packet>
    {
        self.transport.collect_packet()
    }
}

pub(crate) trait Encode<B: BufMut> {
    fn encode(self, dst: &mut B) -> Result<()>;
}

pub(crate) trait Decode<B: Buf> {
    fn decode(src: &mut B) -> Result<Self>
    where
        Self: Sized;
}
