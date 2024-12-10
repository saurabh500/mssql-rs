use super::packet::{Packet, PacketHeader, PacketType, HEADER_BYTES};
use crate::connection::{Decode, Encode};
use crate::TdsError;
use bytes::{BufMut, BytesMut};
use native_tls::TlsConnector;
use native_tls::TlsStream;
use std::io::{Read, Result, Write};
use std::net::TcpStream;
use tracing::{event, Level};

pub(crate) enum TransportStream {
    TcpStream(TcpStream),
    TlsStream(TlsStream<TdsTransport<TcpStream>>),
}

impl TransportStream {
    pub(crate) fn new_tcp_stream(stream: TcpStream) -> Self {
        TransportStream::TcpStream(stream)
    }

    pub(crate) fn new_tls_stream(stream: TlsStream<TdsTransport<TcpStream>>) -> Self {
        TransportStream::TlsStream(stream)
    }

    pub(crate) fn into_inner(self) -> TcpStream {
        match self {
            Self::TcpStream(s) => s,
            Self::TlsStream(mut tls) => tls.get_mut().stream.take().unwrap(),
        }
    }
}

impl Read for TransportStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            TransportStream::TcpStream(stream) => stream.read(buf),
            TransportStream::TlsStream(stream) => stream.read(buf),
        }
    }
}

impl Write for TransportStream {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        match self {
            TransportStream::TcpStream(stream) => stream.write(buf),
            TransportStream::TlsStream(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> Result<()> {
        match self {
            TransportStream::TcpStream(stream) => stream.flush(),
            TransportStream::TlsStream(stream) => stream.flush(),
        }
    }
}

pub(crate) struct TdsTransport<S> {
    stream: Option<S>,
    pending_handshake: bool,
    read_remaining: usize,
}

impl<S> TdsTransport<S> {
    pub(crate) fn new(stream: S) -> Self {
        TdsTransport {
            stream: Some(stream),
            pending_handshake: true,
            read_remaining: 0,
        }
    }

    pub(crate) fn handshake_complete(&mut self) {
        self.pending_handshake = false;
    }
}
impl<S: Read + Write> Read for TdsTransport<S> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.pending_handshake {
            if self.read_remaining == 0 {
                let mut header_buf = [0u8; HEADER_BYTES];
                let read_header: usize = self.stream.as_mut().unwrap().read(&mut header_buf[..])?;
                event!(Level::DEBUG, "Read header {} bytes.", read_header);
                let header = PacketHeader::decode(&mut BytesMut::from(&header_buf[..])).unwrap();
                self.read_remaining = header.length() as usize - HEADER_BYTES;
            }
        }

        let read = self.stream.as_mut().unwrap().read(&mut buf[..])?;
        if self.pending_handshake {
            self.read_remaining -= read;
            event!(
                Level::DEBUG,
                "Read remaining of TLS handshake {} bytes.",
                self.read_remaining
            );
        }
        Ok(read)
    }
}

impl<S: Read + Write> Write for TdsTransport<S> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.pending_handshake {
            let mut data = BytesMut::new();
            data.put(buf);
            let header = PacketHeader::new(PacketType::PreLogin, 0);
            let packet = Packet::new(header, data);
            let mut payload = BytesMut::new();
            packet.encode(&mut payload).unwrap();

            self.stream.as_mut().unwrap().write(&payload)?;
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

pub(crate) fn create_tls_stream<S: Read + Write>(
    host: &str,
    stream: S,
) -> crate::Result<TlsStream<S>> {
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .use_sni(false)
        .build()
        .unwrap();

    let result = connector.connect(host, stream);
    match result {
        Ok(stream) => Ok(stream),
        Err(_e) => Err(TdsError::Message("Handshake failed".to_string())),
    }
}
