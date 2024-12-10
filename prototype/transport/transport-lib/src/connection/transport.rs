mod tls_transport;
mod transport_buffer;

use super::super::parser::Decode;
use crate::TdsError;
use bytes::BytesMut;
use native_tls::TlsStream;
use std::io::{Read, Result, Write};
use std::net::TcpStream;
pub(crate) use tls_transport::TlsTransport;
pub(crate) use transport_buffer::TransportBuffer;

use super::packet::{Packet, PacketHeader, HEADER_BYTES};

pub(crate) enum Transport {
    TcpStream(TcpStream),
    TlsStream(TlsStream<TlsTransport<TcpStream>>),
    None,
}

impl Transport {
    pub(crate) fn into_tcp(self) -> Option<TcpStream> {
        match self {
            Self::TcpStream(s) => Some(s),
            Self::TlsStream(mut tls) => tls.get_mut().stream.take(),
            Self::None => None,
        }
    }

    pub fn collect_packet(&mut self) -> crate::Result<Packet> {
        let mut buf = BytesMut::zeroed(HEADER_BYTES);
        let size = self.read(buf.as_mut())?;

        if size != HEADER_BYTES {
            return Err(TdsError::Message("Invalid packet header".into()));
        }

        let header = PacketHeader::decode(&mut buf)?;
        let length = header.length() as usize;

        let mut payload = BytesMut::zeroed(length - HEADER_BYTES);
        let size = self.read(payload.as_mut())?;

        if size != (length - HEADER_BYTES) {
            return Err(TdsError::Message("Invalid packet length".into()));
        }

        Ok(Packet::new(header, payload))
    }
}

impl Write for Transport {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        match self {
            Transport::TcpStream(stream) => stream.write(buf),
            Transport::TlsStream(stream) => stream.write(buf),
            Transport::None => unreachable!(),
        }
    }

    fn flush(&mut self) -> Result<()> {
        match self {
            Transport::TcpStream(stream) => stream.flush(),
            Transport::TlsStream(stream) => stream.flush(),
            Transport::None => unreachable!(),
        }
    }
}

impl Read for Transport {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            Transport::TcpStream(stream) => stream.read(buf),
            Transport::TlsStream(stream) => stream.read(buf),
            Transport::None => unreachable!(),
        }
    }
}
