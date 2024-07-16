pub mod builder;
mod transport;
pub(crate) mod packet;
pub(crate) mod token;

use crate::TdsError;
use super::{Result,EncryptionLevel};
use super::parser::{Encode,Decode};
use transport::Transport;
use transport::TransportBuffer;
use packet::{PacketHeader, PacketType, Packet, HEADER_BYTES};

use tracing::{event, Level};
use std::io::Write;
use bytes::{Buf, BytesMut};

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
    is_last: bool,
    buf: BytesMut,
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
            is_last: true,
            buf: BytesMut::new(),
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

    pub(crate) fn send<E>(&mut self, ty: PacketType, item: E) -> Result<()>
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

    pub(crate) fn collect_token_packet(&mut self) -> Result<()>
    {
        if !self.buf.is_empty() {
            event!(Level::WARN, "Loading a packet when bufer has not been parsed.");
        }

        self.buf.truncate(0);
        event!(Level::TRACE, "Collecting packet.");
        let packet = self.transport.collect_packet()?;
        let (header, payload) = packet.into_parts();
        event!(Level::TRACE, "Received packet {:?} with size {}.", header, payload.len());
        self.buf.extend(payload);
        self.is_last = header.is_last();
        Ok(())
    }

    fn esure_bytes(&mut self, size: usize) -> Result<()> {

        while self.buf.len() < size {
            let packet = self.transport.collect_packet()?;
            let (header, payload) = packet.into_parts();
            self.buf.extend(payload);

            self.is_last = header.is_last();
            if self.is_last {
                break;
            }
        }

        if self.buf.len() < size {
            return Err(TdsError::Message(format!("No bytes {} != {}", self.buf.len(), size).into()));
        }

        Ok(())
    }
}

macro_rules! buf_get {
    ($this:ident, $typ:tt::$conv:tt) => {{
        const SIZE: usize = std::mem::size_of::<$typ>();
        $this.esure_bytes(SIZE)?;

        let b: [u8; SIZE] = $this.buf[..SIZE].try_into().unwrap();
        $this.buf.advance(SIZE);
        return Ok($typ::$conv(b));
    }}
}

impl TransportBuffer for Connection {
    fn get_u8(&mut self) -> Result<u8> {
        buf_get!(self, u8::from_be_bytes);
    }

    fn get_u16_le(&mut self) -> Result<u16> {
        buf_get!(self, u16::from_le_bytes);
    }

    fn get_u32(&mut self) -> Result<u32> {
        buf_get!(self, u32::from_be_bytes);
    }

    fn get_u32_le(&mut self) -> Result<u32> {
        buf_get!(self, u32::from_le_bytes);
    }

    fn split_to(&mut self, size: usize) -> Result<BytesMut> {
        self.esure_bytes(size)?;
        Ok(self.buf.split_to(size))
    }

    fn advance(&mut self, size: usize) -> Result<()> {
        self.esure_bytes(size)?;
        self.buf.advance(size);
        Ok(())   
    }

    fn is_eof(&self) -> bool {
        self.is_last && self.buf.is_empty()
    }
}
