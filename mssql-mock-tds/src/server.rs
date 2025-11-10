// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Mock TDS Server implementation

use crate::protocol::{
    PACKET_HEADER_SIZE, PacketHeader, PacketType, ProtocolError, build_done_token,
    build_error_response, build_login_ack, build_prelogin_response, build_query_result,
    parse_sql_batch,
};
use crate::query_response::QueryRegistry;
use bytes::BytesMut;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Mock TDS Server
pub struct MockTdsServer {
    listener: TcpListener,
    local_addr: SocketAddr,
    query_registry: Arc<Mutex<QueryRegistry>>,
}

impl MockTdsServer {
    /// Create a new mock TDS server
    pub async fn new(addr: &str) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;

        info!("Mock TDS Server listening on {}", local_addr);

        Ok(Self {
            listener,
            local_addr,
            query_registry: Arc::new(Mutex::new(QueryRegistry::new())),
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

        loop {
            let (socket, addr) = listener.accept().await?;
            info!("New connection from {}", addr);

            let registry_clone = Arc::clone(&registry);

            // Spawn a task to handle this connection
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, addr, registry_clone).await {
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

        tokio::select! {
            result = async {
                loop {
                    let listener = listener.lock().await;
                    match listener.accept().await {
                        Ok((socket, addr)) => {
                            info!("New connection from {}", addr);
                            drop(listener); // Release lock before spawning

                            let registry_clone = Arc::clone(&registry);

                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(socket, addr, registry_clone).await {
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

/// Handle a single client connection
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
