use std::io::{Read, Write,Result};
use bytes::{BufMut, BytesMut};
use tracing::{event, Level};
use crate::connection::{PacketHeader, PacketType, Packet, HEADER_BYTES,Decode,Encode};

pub(crate) struct TlsTransport<S> {
    pub(crate) stream: Option<S>,
    pending_handshake: bool,
    read_remaining: usize,
}

impl<S> TlsTransport<S> {
    pub(crate) fn new(stream: S) -> Self {
        TlsTransport {
            stream: Some(stream),
            pending_handshake: true,
            read_remaining: 0,
        }
    }

    pub(crate) fn handshake_complete(&mut self) {
        self.pending_handshake = false;
    }
}
impl<S: Read + Write> Read for TlsTransport<S> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.pending_handshake {
            if self.read_remaining == 0 {
                let mut header_buf = [0u8; HEADER_BYTES];
                let read_header: usize = self.stream.as_mut().unwrap().read(&mut header_buf[..])?;
                event!(Level::DEBUG, "Read header {} bytes.", read_header);
                let header = PacketHeader::decode(&mut BytesMut::from(&header_buf[..])).unwrap();
                event!(Level::DEBUG, "TLS PacketHeader {:?}.", header);
                self.read_remaining = header.length() as usize - HEADER_BYTES;
            }
        }

        let read = self.stream.as_mut().unwrap().read(&mut buf[..])?;
        if self.pending_handshake {
            self.read_remaining -= read;
            event!(Level::DEBUG, "Read remaining of TLS handshake {} bytes.", read);
        }
        Ok(read)
    }
}

impl<S: Read + Write> Write for TlsTransport<S> {  
    fn write(&mut self, buf: &[u8]) -> Result<usize> {

        if self.pending_handshake {
            event!(Level::DEBUG, "Write header for TLS.");
            let mut data = BytesMut::new();
            data.put(buf);
            let header = PacketHeader::new(PacketType::PreLogin, 0);
            let packet = Packet::new(header, data);
            let mut payload = BytesMut::new();
            packet.encode(&mut payload).unwrap();

            self.stream.as_mut().unwrap().write(&payload)?;
            event!(Level::DEBUG, "Wrote TLS stream {} bytes.", buf.len());
            Ok(buf.len())
        } else {
            self.stream.as_mut().unwrap().write(buf)?;
            Ok(buf.len())
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.stream.as_mut().unwrap().flush()
    }
}
