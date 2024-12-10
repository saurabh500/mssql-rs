use crate::decode::Decode;
use crate::encode::Encode;
use crate::header::{PacketHeader, PacketStatus, PacketType};
use crate::HEADER_BYTES;
use async_native_tls::{TlsConnector, TlsStream};
use async_std::net::TcpStream;
use bytes::BytesMut;
use futures_util::io::{AsyncRead, AsyncWrite};
use futures_util::ready;
use std::{
    cmp, io,
    pin::Pin,
    task::{self, Poll},
};
use tracing::{event, Level};

pub(crate) enum Protocol {
    TcpStream(TcpStream),
    Tls(TlsStream<TlsPreloginWrapper<TcpStream>>),
}

impl Protocol {
    pub fn into_inner(self) -> TcpStream {
        match self {
            Self::TcpStream(s) => s,
            Self::Tls(mut tls) => tls.get_mut().stream.take().unwrap(),
        }
    }
}

impl AsyncRead for Protocol {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Protocol::TcpStream(s) => Pin::new(s).poll_read(cx, buf),
            Protocol::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Protocol {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Protocol::TcpStream(s) => Pin::new(s).poll_write(cx, buf),
            Protocol::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Protocol::TcpStream(s) => Pin::new(s).poll_flush(cx),
            Protocol::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Protocol::TcpStream(s) => Pin::new(s).poll_close(cx),
            Protocol::Tls(s) => Pin::new(s).poll_close(cx),
        }
    }
}

/// On TLS handshake, the server expects to get and sends back normal TDS
/// packets. To use a common TLS library, we must implement a wrapper for
/// packet handling on this stage.
///
/// What it does is it interferes on handshake for TDS packet handling,
/// and when complete, just passes the calls to the underlying connection.
pub(crate) struct TlsPreloginWrapper<S> {
    stream: Option<S>,
    pending_handshake: bool,

    header_buf: [u8; HEADER_BYTES],
    header_pos: usize,
    read_remaining: usize,

    wr_buf: Vec<u8>,
    header_written: bool,
}

impl<S> TlsPreloginWrapper<S> {
    pub fn new(stream: S) -> Self {
        TlsPreloginWrapper {
            stream: Some(stream),
            pending_handshake: true,

            header_buf: [0u8; HEADER_BYTES],
            header_pos: 0,
            read_remaining: 0,
            wr_buf: vec![0u8; HEADER_BYTES],
            header_written: false,
        }
    }

    pub fn handshake_complete(&mut self) {
        self.pending_handshake = false;
    }
}

/// When reading a packet while the handshake is not complete,
/// we must remove the TDS header and send only TLS handshake data.
impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncRead for TlsPreloginWrapper<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // Normal operation does not need any extra treatment, we handle packets
        // in the codec.
        if !self.pending_handshake {
            return Pin::new(&mut self.stream.as_mut().unwrap()).poll_read(cx, buf);
        }

        let inner = self.get_mut();

        // Read the headers separately and do not send them to the Tls
        // connection handling.
        if !inner.header_buf[inner.header_pos..].is_empty() {
            while !inner.header_buf[inner.header_pos..].is_empty() {
                let read = ready!(Pin::new(inner.stream.as_mut().unwrap())
                    .poll_read(cx, &mut inner.header_buf[inner.header_pos..]))?;

                event!(Level::TRACE, "Read... {}", read);
                if read == 0 {
                    return Poll::Ready(Ok(0));
                }
                inner.header_pos += read;
            }

            let header = PacketHeader::decode(&mut BytesMut::from(&inner.header_buf[..]))
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

            // We only get pre-login packets in the handshake process.
            assert_eq!(header.r#type(), PacketType::PreLogin);

            // And we know from this point on how much data we should expect
            inner.read_remaining = header.length() as usize - HEADER_BYTES;

            event!(
                Level::DEBUG,
                "TLS handshake. Reading packet of {} bytes",
                inner.read_remaining,
            );
        }

        let max_read = cmp::min(inner.read_remaining, buf.len());

        // TLS connector gets whatever we have after the header.
        let read = ready!(
            Pin::new(&mut inner.stream.as_mut().unwrap()).poll_read(cx, &mut buf[..max_read])
        )?;

        inner.read_remaining -= read;

        // All data is read, after this we're expecting a new header.
        if inner.read_remaining == 0 {
            inner.header_pos = 0;
        }

        Poll::Ready(Ok(read))
    }
}

/// When writing a packet while the handshake is not complete,
/// we must add a  TDS header and send only a packed with TDS header + TLS handshake data.
impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncWrite for TlsPreloginWrapper<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Normal operation does not need any extra treatment, we handle
        // packets in the codec.
        if !self.pending_handshake {
            return Pin::new(&mut self.stream.as_mut().unwrap()).poll_write(cx, buf);
        }

        // Buffering data.
        self.wr_buf.extend_from_slice(buf);

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        let inner = self.get_mut();

        // If on handshake mode, wraps the data to a TDS packet before sending.
        if inner.pending_handshake && inner.wr_buf.len() > HEADER_BYTES {
            if !inner.header_written {
                let mut header = PacketHeader::new(inner.wr_buf.len(), 0);

                header.set_type(PacketType::PreLogin);
                header.set_status(PacketStatus::EndOfMessage);

                header
                    .encode(&mut &mut inner.wr_buf[0..HEADER_BYTES])
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "Could not encode header.")
                    })?;

                inner.header_written = true;
            }

            while !inner.wr_buf.is_empty() {
                event!(
                    Level::DEBUG,
                    "TLS handshake. Writing a packet of {} bytes",
                    inner.wr_buf.len(),
                );

                let written = ready!(
                    Pin::new(&mut inner.stream.as_mut().unwrap()).poll_write(cx, &inner.wr_buf)
                )?;

                inner.wr_buf.drain(..written);
            }

            inner.wr_buf.resize(HEADER_BYTES, 0);
            inner.header_written = false;
        }

        Pin::new(&mut inner.stream.as_mut().unwrap()).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream.as_mut().unwrap()).poll_close(cx)
    }
}

pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
    stream: S,
    host: &str,
) -> crate::Result<TlsStream<S>> {
    let mut builder = TlsConnector::new();

    event!(
        Level::INFO,
        "Trusting the server certificate without validation."
    );

    builder = builder.danger_accept_invalid_certs(true);
    builder = builder.danger_accept_invalid_hostnames(true);
    builder = builder.use_sni(false);

    let return_value = builder.connect(host, stream).await?;
    Ok(return_value)
}
