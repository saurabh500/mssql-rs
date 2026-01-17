// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Mock TDS Server implementation

use crate::protocol::{
    PACKET_HEADER_SIZE, PacketHeader, PacketType, ProtocolError, build_done_token,
    build_error_response, build_login_ack, build_prelogin_response,
    build_prelogin_response_with_encryption, build_query_result, parse_sql_batch,
};
use crate::query_response::QueryRegistry;
use bytes::{BufMut, BytesMut};
use native_tls::Identity;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_native_tls::{TlsAcceptor, TlsStream};
use tracing::{debug, error, info, warn};

/// Mock TDS Server
pub struct MockTdsServer {
    listener: TcpListener,
    local_addr: SocketAddr,
    query_registry: Arc<Mutex<QueryRegistry>>,
    tls_acceptor: Option<TlsAcceptor>,
    /// If true, use TDS 8.0 strict mode where TLS starts immediately
    strict_mode: bool,
}

impl MockTdsServer {
    /// Create a new mock TDS server without TLS
    pub async fn new(addr: &str) -> Result<Self, std::io::Error> {
        Self::new_with_tls(addr, None).await
    }

    /// Create a new mock TDS server with optional TLS support (TDS 7.4 style)
    pub async fn new_with_tls(
        addr: &str,
        identity: Option<Identity>,
    ) -> Result<Self, std::io::Error> {
        Self::new_internal(addr, identity, false).await
    }

    /// Create a new mock TDS server with strict/TDS 8.0 mode
    /// In strict mode, TLS handshake happens immediately before any TDS packets
    pub async fn new_with_strict_tls(
        addr: &str,
        identity: Identity,
    ) -> Result<Self, std::io::Error> {
        Self::new_internal(addr, Some(identity), true).await
    }

    /// Internal constructor
    async fn new_internal(
        addr: &str,
        identity: Option<Identity>,
        strict_mode: bool,
    ) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;

        let tls_acceptor = identity.map(|id| {
            let acceptor = native_tls::TlsAcceptor::builder(id)
                .build()
                .expect("Failed to build TLS acceptor");
            TlsAcceptor::from(acceptor)
        });

        if strict_mode {
            info!(
                "Mock TDS Server listening on {} with TDS 8.0 strict TLS mode",
                local_addr
            );
        } else if tls_acceptor.is_some() {
            info!(
                "Mock TDS Server listening on {} with TLS enabled",
                local_addr
            );
        } else {
            info!("Mock TDS Server listening on {} (no TLS)", local_addr);
        }

        Ok(Self {
            listener,
            local_addr,
            query_registry: Arc::new(Mutex::new(QueryRegistry::new())),
            tls_acceptor,
            strict_mode,
        })
    }

    /// Get a reference to the query registry for registering custom responses
    pub fn query_registry(&self) -> Arc<Mutex<QueryRegistry>> {
        Arc::clone(&self.query_registry)
    }

    /// Get the local address the server is bound to
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Run the server (accepts connections in a loop)
    pub async fn run(self) -> Result<(), std::io::Error> {
        let listener = Arc::new(self.listener);
        let registry = self.query_registry;
        let tls_acceptor = self.tls_acceptor.map(Arc::new);
        let strict_mode = self.strict_mode;

        loop {
            let (socket, addr) = listener.accept().await?;
            info!("New connection from {}", addr);

            let registry_clone = Arc::clone(&registry);
            let tls_acceptor_clone = tls_acceptor.clone();

            // Spawn a task to handle this connection
            tokio::spawn(async move {
                if let Err(e) = handle_connection_with_tls(
                    socket,
                    addr,
                    registry_clone,
                    tls_acceptor_clone,
                    strict_mode,
                )
                .await
                {
                    error!("Error handling connection from {}: {}", addr, e);
                }
            });
        }
    }

    /// Run the server with a shutdown signal
    pub async fn run_with_shutdown(
        self,
        shutdown: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), std::io::Error> {
        let listener = Arc::new(Mutex::new(self.listener));
        let registry = self.query_registry;
        let tls_acceptor = self.tls_acceptor.map(Arc::new);
        let strict_mode = self.strict_mode;

        tokio::select! {
            result = async {
                loop {
                    let listener = listener.lock().await;
                    match listener.accept().await {
                        Ok((socket, addr)) => {
                            info!("New connection from {}", addr);
                            drop(listener); // Release lock before spawning

                            let registry_clone = Arc::clone(&registry);
                            let tls_acceptor_clone = tls_acceptor.clone();

                            tokio::spawn(async move {
                                if let Err(e) = handle_connection_with_tls(socket, addr, registry_clone, tls_acceptor_clone, strict_mode).await {
                                    error!("Error handling connection from {}: {}", addr, e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("Error accepting connection: {}", e);
                            return Err(e);
                        }
                    }
                }
            } => result,
            _ = shutdown => {
                info!("Shutdown signal received, stopping server");
                Ok(())
            }
        }
    }
}

/// Handle a connection with optional TLS support
async fn handle_connection_with_tls(
    socket: TcpStream,
    addr: SocketAddr,
    query_registry: Arc<Mutex<QueryRegistry>>,
    tls_acceptor: Option<Arc<TlsAcceptor>>,
    strict_mode: bool,
) -> Result<(), ProtocolError> {
    if strict_mode {
        // TDS 8.0 Strict mode: TLS handshake happens immediately on the socket
        // No TDS wrapping - raw TLS handshake followed by TDS packets over TLS
        let tls_acceptor = tls_acceptor.ok_or_else(|| {
            ProtocolError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Strict mode requires TLS acceptor",
            ))
        })?;

        debug!("Starting TDS 8.0 strict mode TLS handshake for {}", addr);

        let tls_stream = tls_acceptor.accept(socket).await.map_err(|e| {
            error!("TLS handshake failed in strict mode: {}", e);
            ProtocolError::Io(std::io::Error::other(format!(
                "TLS handshake failed: {}",
                e
            )))
        })?;

        info!("TLS handshake successful for {} (strict mode)", addr);
        handle_strict_encrypted_connection(tls_stream, addr, query_registry).await
    } else {
        // TDS 7.4 mode: First handle PreLogin, then optionally do TDS-wrapped TLS
        let (prelogin_socket, should_encrypt) =
            handle_prelogin_negotiation(socket, addr, tls_acceptor.is_some()).await?;

        if should_encrypt && tls_acceptor.is_some() {
            // For TDS 7.4, the client wraps TLS handshake data in TDS PreLogin packets.
            // We need to use TdsTlsWrapper to unwrap TDS packets and extract TLS data.
            use crate::tds_tls_wrapper::TdsTlsWrapper;

            let tds_wrapper = TdsTlsWrapper::new(prelogin_socket);

            // Perform TLS handshake over the TDS-wrapped stream
            let tls_stream = tls_acceptor
                .unwrap()
                .accept(tds_wrapper)
                .await
                .map_err(|e| {
                    error!("TLS handshake failed: {}", e);
                    ProtocolError::Io(std::io::Error::other(format!(
                        "TLS handshake failed: {}",
                        e
                    )))
                })?;

            info!("TLS handshake successful for {}", addr);
            handle_encrypted_tds_wrapped_connection(tls_stream, addr, query_registry).await
        } else {
            // Continue without encryption
            handle_unencrypted_connection(prelogin_socket, addr, query_registry).await
        }
    }
}

/// Handle PreLogin packet to negotiate encryption
async fn handle_prelogin_negotiation(
    mut socket: TcpStream,
    addr: SocketAddr,
    supports_tls: bool,
) -> Result<(TcpStream, bool), ProtocolError> {
    let mut buffer = BytesMut::with_capacity(4096);

    // Read PreLogin packet
    let n = socket.read_buf(&mut buffer).await?;
    if n == 0 {
        return Err(ProtocolError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Connection closed during PreLogin",
        )));
    }

    debug!("Received {} bytes from {} (PreLogin)", n, addr);

    if buffer.len() < PACKET_HEADER_SIZE {
        return Err(ProtocolError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Incomplete PreLogin packet",
        )));
    }

    let mut buf_clone = buffer.clone();
    let header = PacketHeader::parse(&mut buf_clone)?;

    if header.packet_type != PacketType::PreLogin {
        return Err(ProtocolError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected PreLogin, got {:?}", header.packet_type),
        )));
    }

    debug!("Handling PreLogin negotiation");

    // Build PreLogin response based on TLS support
    let response = build_prelogin_response_with_encryption(supports_tls);

    debug!("Sending {} bytes PreLogin response", response.len());
    socket.write_all(&response).await?;

    // Return socket and whether client should encrypt
    Ok((socket, supports_tls))
}

/// Handle strict mode encrypted connection (TDS 8.0)
/// In strict mode, TLS is established first, then PreLogin and all other TDS packets
/// are exchanged over the encrypted channel.
async fn handle_strict_encrypted_connection(
    mut socket: TlsStream<TcpStream>,
    addr: SocketAddr,
    query_registry: Arc<Mutex<QueryRegistry>>,
) -> Result<(), ProtocolError> {
    let mut buffer = BytesMut::with_capacity(4096);
    let mut is_authenticated = false;
    let mut prelogin_handled = false;

    loop {
        // Read data from TLS socket
        let n = socket.read_buf(&mut buffer).await?;

        if n == 0 {
            debug!("TLS connection closed by client {} (strict mode)", addr);
            break;
        }

        debug!("Received {} encrypted bytes from {} (strict mode)", n, addr);

        // In strict mode, we need to handle PreLogin first, then other packets
        if !prelogin_handled {
            // Check if we have enough data for a packet header
            if buffer.len() >= PACKET_HEADER_SIZE {
                let mut buf_clone = buffer.clone();
                if let Ok(header) = PacketHeader::parse(&mut buf_clone)
                    && header.packet_type == PacketType::PreLogin
                {
                    // Wait for full PreLogin packet
                    if buffer.len() >= header.length as usize {
                        debug!("Handling PreLogin in strict mode");
                        let _ = buffer.split_to(header.length as usize);

                        // Send PreLogin response (encryption is already established)
                        let response = build_prelogin_response();
                        debug!(
                            "Sending {} bytes PreLogin response (strict mode)",
                            response.len()
                        );
                        socket.write_all(&response).await?;
                        prelogin_handled = true;
                        continue;
                    }
                }
            }
            // If we got here and haven't handled PreLogin yet, continue reading
            if !prelogin_handled {
                continue;
            }
        }

        // Process other packets (Login7, SqlBatch, etc.)
        while let Some(response) =
            process_packet(&mut buffer, &query_registry, &mut is_authenticated).await?
        {
            debug!(
                "Sending {} encrypted bytes response (strict mode)",
                response.len()
            );
            socket.write_all(&response).await?;
        }
    }

    Ok(())
}

/// Handle encrypted connection (after TLS handshake)
#[allow(dead_code)]
async fn handle_encrypted_connection(
    mut socket: TlsStream<TcpStream>,
    addr: SocketAddr,
    query_registry: Arc<Mutex<QueryRegistry>>,
) -> Result<(), ProtocolError> {
    let mut buffer = BytesMut::with_capacity(4096);
    let mut is_authenticated = false;

    loop {
        // Read data from TLS socket
        let n = socket.read_buf(&mut buffer).await?;

        if n == 0 {
            debug!("TLS connection closed by client {}", addr);
            break;
        }

        debug!("Received {} encrypted bytes from {}", n, addr);

        // Process packets (same logic as unencrypted)
        while let Some(response) =
            process_packet(&mut buffer, &query_registry, &mut is_authenticated).await?
        {
            debug!("Sending {} encrypted bytes response", response.len());
            socket.write_all(&response).await?;
        }
    }

    Ok(())
}

/// Handle encrypted connection over TDS-wrapped TLS (TDS 7.4 style)
/// After the TLS handshake completes, subsequent TDS packets (Login7, SqlBatch, etc.)
/// are sent encrypted through TLS, but no longer wrapped in PreLogin packets.
async fn handle_encrypted_tds_wrapped_connection(
    mut socket: TlsStream<crate::tds_tls_wrapper::TdsTlsWrapper>,
    addr: SocketAddr,
    query_registry: Arc<Mutex<QueryRegistry>>,
) -> Result<(), ProtocolError> {
    let mut buffer = BytesMut::with_capacity(4096);
    let mut is_authenticated = false;

    loop {
        // Read data from TLS socket (which wraps TdsTlsWrapper)
        let n = socket.read_buf(&mut buffer).await?;

        if n == 0 {
            debug!("TLS connection closed by client {}", addr);
            break;
        }

        debug!(
            "Received {} encrypted bytes from {} (TDS-wrapped TLS)",
            n, addr
        );

        // Process packets (same logic as unencrypted)
        while let Some(response) =
            process_packet(&mut buffer, &query_registry, &mut is_authenticated).await?
        {
            debug!("Sending {} encrypted bytes response", response.len());
            socket.write_all(&response).await?;
        }
    }

    Ok(())
}

/// Handle unencrypted connection (after PreLogin or no TLS)
async fn handle_unencrypted_connection(
    mut socket: TcpStream,
    addr: SocketAddr,
    query_registry: Arc<Mutex<QueryRegistry>>,
) -> Result<(), ProtocolError> {
    let mut buffer = BytesMut::with_capacity(4096);
    let mut is_authenticated = false;

    loop {
        // Read data from plain socket
        let n = socket.read_buf(&mut buffer).await?;

        if n == 0 {
            debug!("Connection closed by client {}", addr);
            break;
        }

        debug!("Received {} bytes from {}", n, addr);

        // Process packets
        while let Some(response) =
            process_packet(&mut buffer, &query_registry, &mut is_authenticated).await?
        {
            debug!("Sending {} bytes response", response.len());
            socket.write_all(&response).await?;
        }
    }

    Ok(())
}

/// Process a single packet from the buffer
async fn process_packet(
    buffer: &mut BytesMut,
    query_registry: &Arc<Mutex<QueryRegistry>>,
    is_authenticated: &mut bool,
) -> Result<Option<BytesMut>, ProtocolError> {
    if buffer.len() < PACKET_HEADER_SIZE {
        return Ok(None);
    }

    // Parse packet header
    let header = {
        let mut buf_clone = buffer.clone();
        match PacketHeader::parse(&mut buf_clone) {
            Ok(h) => h,
            Err(e) => {
                warn!("Failed to parse packet header: {}", e);
                return Ok(None);
            }
        }
    };

    // Check if we have the full packet
    if buffer.len() < header.length as usize {
        debug!(
            "Incomplete packet: have {} bytes, need {}",
            buffer.len(),
            header.length
        );
        return Ok(None);
    }

    // Extract the complete packet
    let packet_data = buffer.split_to(header.length as usize);

    // Process the packet and build response
    let response = match header.packet_type {
        PacketType::Login7 => {
            debug!("Handling Login7");
            *is_authenticated = true;

            // Build response with LoginAck + Done
            let mut response = build_login_ack();
            response.extend_from_slice(&build_done_token(0));

            // Wrap in packet
            let total_length = (PACKET_HEADER_SIZE + response.len()) as u16;
            let mut packet = BytesMut::with_capacity(total_length as usize);
            let resp_header = PacketHeader::new(PacketType::TabularResult, total_length, 1);
            resp_header.write(&mut packet);
            packet.extend_from_slice(&response);

            Some(packet)
        }

        PacketType::SqlBatch => {
            if !*is_authenticated {
                warn!("Received SQL batch before authentication");
                Some(build_error_response("Not authenticated"))
            } else {
                debug!("Handling SQL batch");

                // Extract packet body (skip header)
                let packet_body = &packet_data[PACKET_HEADER_SIZE..];

                // Parse SQL
                match parse_sql_batch(packet_body) {
                    Ok(sql) => {
                        info!("Executing SQL: {}", sql);

                        // Look up query in registry
                        let registry = query_registry.lock().await;
                        if let Some(response_data) = registry.get(&sql) {
                            info!("Found registered response for query");
                            // build_query_result already wraps in a packet, so return directly
                            let packet = build_query_result(response_data);
                            Some(packet)
                        } else {
                            info!("No registered response, returning empty result");
                            // Return DONE token
                            let response = build_done_token(0);

                            let total_length = (PACKET_HEADER_SIZE + response.len()) as u16;
                            let mut packet = BytesMut::with_capacity(total_length as usize);
                            let resp_header =
                                PacketHeader::new(PacketType::TabularResult, total_length, 1);
                            resp_header.write(&mut packet);
                            packet.extend_from_slice(&response);

                            Some(packet)
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse SQL batch: {}", e);
                        Some(build_error_response("Failed to parse SQL"))
                    }
                }
            }
        }

        PacketType::RpcRequest => {
            debug!("Handling RPC request (not fully implemented)");
            // Just return DONE for now
            let response = build_done_token(0);

            let total_length = (PACKET_HEADER_SIZE + response.len()) as u16;
            let mut packet = BytesMut::with_capacity(total_length as usize);
            let resp_header = PacketHeader::new(PacketType::TabularResult, total_length, 1);
            resp_header.write(&mut packet);
            packet.extend_from_slice(&response);

            Some(packet)
        }

        PacketType::Attention => {
            debug!("Received attention/cancel request from client");
            // Attention is a signal to cancel the current operation
            // We respond with a DONE token with ATTENTION status
            let mut response = BytesMut::new();

            // DONE token with DONE_ATTN status (0x20)
            response.put_u8(0xFD); // DONE token
            response.put_u16(0x0020); // Status: DONE_ATTN
            response.put_u16(0x0000); // CurCmd
            response.put_u64_le(0); // RowCount

            let total_length = (PACKET_HEADER_SIZE + response.len()) as u16;
            let mut packet = BytesMut::with_capacity(total_length as usize);
            let resp_header = PacketHeader::new(PacketType::TabularResult, total_length, 1);
            resp_header.write(&mut packet);
            packet.extend_from_slice(&response);

            Some(packet)
        }

        _ => {
            warn!("Unhandled packet type: {:?}", header.packet_type);
            None
        }
    };

    Ok(response)
}

/// Handle a single client connection (legacy, non-TLS)
#[allow(dead_code)]
async fn handle_connection(
    mut socket: TcpStream,
    addr: SocketAddr,
    query_registry: Arc<Mutex<QueryRegistry>>,
) -> Result<(), ProtocolError> {
    let mut buffer = BytesMut::with_capacity(4096);
    let mut is_authenticated = false;

    loop {
        // Read data from socket
        let n = socket.read_buf(&mut buffer).await?;

        if n == 0 {
            debug!("Connection closed by client {}", addr);
            break;
        }

        debug!("Received {} bytes from {}", n, addr);

        // Process all complete packets in the buffer
        while buffer.len() >= PACKET_HEADER_SIZE {
            // Parse packet header
            let header = {
                let mut buf_clone = buffer.clone();
                match PacketHeader::parse(&mut buf_clone) {
                    Ok(h) => h,
                    Err(e) => {
                        warn!("Failed to parse packet header: {}", e);
                        break;
                    }
                }
            };

            // Check if we have the full packet
            if buffer.len() < header.length as usize {
                debug!(
                    "Incomplete packet: have {} bytes, need {}",
                    buffer.len(),
                    header.length
                );
                break;
            }

            // Extract the complete packet
            let packet_data = buffer.split_to(header.length as usize);

            // Process the packet
            let response = match header.packet_type {
                PacketType::PreLogin => {
                    debug!("Handling PreLogin");
                    Some(build_prelogin_response())
                }

                PacketType::Login7 => {
                    debug!("Handling Login7");
                    is_authenticated = true;

                    // Build response with LoginAck + EnvChange + Done
                    let mut response = build_login_ack();
                    response.extend_from_slice(&build_done_token(0));

                    // Wrap in packet
                    let total_length = (PACKET_HEADER_SIZE + response.len()) as u16;
                    let mut packet = BytesMut::with_capacity(total_length as usize);
                    let resp_header = PacketHeader::new(PacketType::TabularResult, total_length, 1);
                    resp_header.write(&mut packet);
                    packet.extend_from_slice(&response);

                    Some(packet)
                }

                PacketType::SqlBatch => {
                    if !is_authenticated {
                        warn!("Received SQL batch before authentication");
                        Some(build_error_response("Not authenticated"))
                    } else {
                        debug!("Handling SQL batch");

                        // Extract packet body (skip header)
                        let packet_body = &packet_data[PACKET_HEADER_SIZE..];

                        // Parse SQL
                        match parse_sql_batch(packet_body) {
                            Ok(sql) => {
                                info!("Executing SQL: {}", sql);

                                // Look up query in registry
                                let registry = query_registry.lock().await;
                                if let Some(response) = registry.get(&sql) {
                                    Some(build_query_result(response))
                                } else if sql.to_uppercase().starts_with("SELECT") {
                                    // Return empty result set with DONE for unknown SELECT queries
                                    let mut response = BytesMut::new();
                                    response.extend_from_slice(&build_done_token(0));

                                    let total_length = (PACKET_HEADER_SIZE + response.len()) as u16;
                                    let mut packet = BytesMut::with_capacity(total_length as usize);
                                    let resp_header = PacketHeader::new(
                                        PacketType::TabularResult,
                                        total_length,
                                        1,
                                    );
                                    resp_header.write(&mut packet);
                                    packet.extend_from_slice(&response);

                                    Some(packet)
                                } else {
                                    // For other commands, just return DONE
                                    let response = build_done_token(0);

                                    let total_length = (PACKET_HEADER_SIZE + response.len()) as u16;
                                    let mut packet = BytesMut::with_capacity(total_length as usize);
                                    let resp_header = PacketHeader::new(
                                        PacketType::TabularResult,
                                        total_length,
                                        1,
                                    );
                                    resp_header.write(&mut packet);
                                    packet.extend_from_slice(&response);

                                    Some(packet)
                                }
                            }
                            Err(e) => {
                                error!("Failed to parse SQL batch: {}", e);
                                Some(build_error_response("Failed to parse SQL"))
                            }
                        }
                    }
                }

                PacketType::RpcRequest => {
                    debug!("Handling RPC request (not fully implemented)");
                    // Just return DONE for now
                    let response = build_done_token(0);

                    let total_length = (PACKET_HEADER_SIZE + response.len()) as u16;
                    let mut packet = BytesMut::with_capacity(total_length as usize);
                    let resp_header = PacketHeader::new(PacketType::TabularResult, total_length, 1);
                    resp_header.write(&mut packet);
                    packet.extend_from_slice(&response);

                    Some(packet)
                }

                _ => {
                    warn!("Unhandled packet type: {:?}", header.packet_type);
                    None
                }
            };

            // Send response if we have one
            if let Some(response_data) = response {
                debug!("Sending {} bytes response", response_data.len());
                socket.write_all(&response_data).await?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_creation() {
        let server = MockTdsServer::new("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr();
        assert!(addr.port() > 0);
    }
}
