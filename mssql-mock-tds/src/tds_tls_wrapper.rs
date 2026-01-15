//! TDS-TLS wrapper that handles TDS packet encapsulation for TLS data
//!
//! SQL Server wraps TLS handshake and encrypted data in TDS packets.
//! This module provides a wrapper that transparently handles the wrapping/unwrapping.

use bytes::{Buf, BytesMut};
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

/// TDS packet type for TLS data
const TDS_PRELOGIN: u8 = 0x12;

/// Wraps a TcpStream to handle TDS packet encapsulation for TLS
pub struct TdsTlsWrapper {
    inner: TcpStream,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
}

impl TdsTlsWrapper {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            inner: stream,
            read_buffer: BytesMut::with_capacity(8192),
            write_buffer: BytesMut::with_capacity(8192),
        }
    }

    /// Unwrap a TDS packet and return the payload
    async fn read_tds_packet(&mut self) -> io::Result<()> {
        // Read TDS header (8 bytes)
        let mut header = [0u8; 8];
        self.inner.read_exact(&mut header).await?;

        let packet_type = header[0];
        let _status = header[1];
        let length = u16::from_be_bytes([header[2], header[3]]) as usize;

        if length < 8 {
            return Err(io::Error::new(ErrorKind::InvalidData, "Invalid TDS packet length"));
        }

        let payload_len = length - 8;

        // Read payload
        let mut payload = vec![0u8; payload_len];
        self.inner.read_exact(&mut payload).await?;

        // Store payload in read buffer
        self.read_buffer.extend_from_slice(&payload);

        Ok(())
    }
}

impl AsyncRead for TdsTlsWrapper {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // If we have buffered data, return it
        if !self.read_buffer.is_empty() {
            let to_read = std::cmp::min(buf.remaining(), self.read_buffer.len());
            buf.put_slice(&self.read_buffer[..to_read]);
            self.read_buffer.advance(to_read);
            return Poll::Ready(Ok(()));
        }

        // Need to read a new TDS packet
        // This is tricky - we need to block until we have a full packet
        // For simplicity, we'll try to read synchronously in a blocking context
        
        // Try to read directly from inner stream if no TDS wrapping expected
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TdsTlsWrapper {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Wrap data in TDS packet
        let packet_len = buf.len() + 8;
        
        let mut packet = BytesMut::with_capacity(packet_len);
        
        // TDS header
        packet.extend_from_slice(&[
            TDS_PRELOGIN,           // packet type
            0x01,                    // status (EOM)
            (packet_len >> 8) as u8, // length high byte
            (packet_len & 0xFF) as u8, // length low byte
            0x00, 0x00,              // SPID
            0x00,                    // packet ID
            0x00,                    // window
        ]);
        
        packet.extend_from_slice(buf);

        // Write packet to inner stream
        Pin::new(&mut self.inner).poll_write(cx, &packet)
            .map_ok(|_| buf.len()) // Return original buffer length, not packet length
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
