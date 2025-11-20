// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::connection::client_context::{IPAddressPreference, TransportContext};
use crate::connection::transport::buffers::TdsReadBuffer;
use crate::connection::transport::ssl_handler::SslHandler;
use crate::connection_provider::tds_connection_provider::PARSER_REGISTRY;
use crate::core::{
    CancelHandle, EncryptionOptions, EncryptionSetting, NegotiatedEncryptionSetting, TdsResult,
};
use crate::error::Error::{OperationCancelledError, TimeoutError};
use crate::error::TimeoutErrorType;
use crate::handler::handler_factory::SessionSettings;
use crate::io::packet_reader::{PacketReader, TdsPacketReader};
use crate::io::packet_writer::PacketWriter;
use crate::io::reader_writer::{NetworkReader, NetworkReaderWriter, NetworkWriter};
use crate::io::token_stream::{
    ParserContext, TdsTokenStreamReader, TokenParserRegistry, TokenParsers,
};
use crate::message::attention::AttentionRequest;
use crate::message::login_options::TdsVersion;
use crate::message::messages::Request;
use crate::token::parsers::TokenParser;
use crate::token::tokens::{DoneStatus, TokenType, Tokens};
use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder, LittleEndian};
use std::cmp::min;
use std::io::Error;
use std::io::ErrorKind::{self, UnexpectedEof};
use std::net::ToSocketAddrs;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{self, TcpStream};
use tokio::time::timeout;
use tracing::{debug, event, info, trace};

#[cfg(windows)]
use crate::connection::transport::named_pipes::{
    create_named_pipe_transport, open_named_pipe_with_retry,
};

pub(crate) const PRE_NEGOTIATED_PACKET_SIZE: u32 = 4096;

pub(crate) async fn create_transport(
    ipaddress_preference: IPAddressPreference,
    tds_version: TdsVersion,
    transport_context: &TransportContext,
    encryption_options: EncryptionOptions,
) -> TdsResult<Box<NetworkTransport>> {
    let encryption_mode = encryption_options.mode;
    let stream = match &transport_context {
        TransportContext::Tcp { host, port } => {
            info!("Connecting to TCP transport: {}:{}", host, port);

            // This will cause the DNS resolution of the addresses.
            let mut socket_addresses = (host.as_str(), *port).to_socket_addrs()?;

            let mut last_error = None;
            let mut tcp_stream = None;

            // Sort the address list based on the IP address preference
            match ipaddress_preference {
                IPAddressPreference::UsePlatformDefault => {
                    // Do nothing. Use whatever the OS returns.
                    trace!("Using platform default IP address preference");
                }
                IPAddressPreference::IPv4First => {
                    let mut addresses: Vec<_> = socket_addresses.collect();
                    // Sort IPv4 addresses first
                    addresses.sort_by_key(|a| a.is_ipv6());
                    socket_addresses = addresses.into_iter();
                    trace!("IPv4 addresses first");
                }
                IPAddressPreference::IPv6First => {
                    let mut addresses: Vec<_> = socket_addresses.collect();
                    // Sort IPv6 addresses first
                    addresses.sort_by_key(|b| std::cmp::Reverse(b.is_ipv6()));
                    socket_addresses = addresses.into_iter();
                    trace!("IPv6 addresses first");
                }
            }

            info!("Socket addresses: {:?}", socket_addresses);

            for socket_address in socket_addresses {
                let socket = if socket_address.is_ipv6() {
                    net::TcpSocket::new_v6()?
                } else {
                    net::TcpSocket::new_v4()?
                };

                // The defaults for the SQL Server clients are at
                // https://learn.microsoft.com/en-us/sql/tools/configuration-manager/client-protocols-tcp-ip-properties-protocol-tab?view=sql-server-ver16
                let keep_alive_settings = socket2::TcpKeepalive::new()
                    .with_time(Duration::from_millis(30_000))
                    .with_interval(Duration::from_millis(1_000));

                let socket2_socket = socket2::SockRef::from(&socket);
                socket2_socket.set_tcp_keepalive(&keep_alive_settings)?;
                socket2_socket.set_nodelay(true)?;

                tcp_stream = match socket.connect(socket_address).await {
                    Ok(stream) => {
                        info!("Connected to TCP transport: {}:{}", host, port);
                        Some(stream)
                    }
                    Err(e) => {
                        last_error = Some(e);
                        None
                    }
                };
                if tcp_stream.is_some() {
                    break;
                }
            }

            // We don't have a valid TCP stream, so we need to return the last error.
            if tcp_stream.is_none() {
                return Err(crate::error::Error::from(last_error.unwrap()));
            }
            tcp_stream.unwrap()
        }
        #[cfg(windows)]
        TransportContext::NamedPipe { pipe_name } => {
            info!("Connecting to Named Pipe: {}", pipe_name);

            // Open Named Pipe with retry logic for ERROR_PIPE_BUSY
            let pipe_client = open_named_pipe_with_retry(pipe_name).await?;

            info!("Connected to Named Pipe: {}", pipe_name);
            return create_named_pipe_transport(
                pipe_client,
                transport_context,
                encryption_options,
                encryption_mode,
            )
            .await;
        }
        #[cfg(not(windows))]
        TransportContext::NamedPipe { .. } => {
            return Err(crate::error::Error::from(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Named Pipes are only supported on Windows",
            )));
        }
        #[cfg(windows)]
        TransportContext::SharedMemory { instance_name } => {
            // Shared Memory protocol is implemented as Named Pipes with a special path format.
            // For SQL Server 2005+, SharedMemory is actually LPC-over-Named-Pipes using the path:
            // \\.\pipe\SQLLocal\<INSTANCE_NAME>
            //
            // This only works for localhost connections and does not support clustered instances.

            // Default to MSSQLSERVER for empty instance name (matching SQL Server behavior)
            let actual_instance = if instance_name.is_empty() {
                "MSSQLSERVER"
            } else {
                instance_name.as_str()
            };

            info!(
                "Connecting via Shared Memory (LPC-over-Named-Pipes) to instance: {}",
                actual_instance
            );

            // Construct the pipe path: \\.\.\pipe\SQLLocal\<instance>
            let pipe_name = format!(r"\\.\pipe\SQLLocal\{actual_instance}");

            info!("Connecting to Shared Memory pipe: {}", pipe_name);

            // Open Named Pipe with retry logic for ERROR_PIPE_BUSY
            let pipe_client = open_named_pipe_with_retry(&pipe_name).await?;

            info!("Connected to Shared Memory (LPC-over-NP): {}", pipe_name);
            return create_named_pipe_transport(
                pipe_client,
                transport_context,
                encryption_options,
                encryption_mode,
            )
            .await;
        }
        #[cfg(not(windows))]
        TransportContext::SharedMemory { .. } => {
            return Err(crate::error::Error::from(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Shared Memory is only supported on Windows",
            )));
        }
    };

    match tds_version {
        TdsVersion::V7_4 => {
            // TDS 7.4 starts with unencrypted streams that could get encrypted as part of prelogin
            // negotiation. TLS must be wrapped in TDS packets for this version.
            let base_stream: Box<dyn Stream> = Box::new(stream);

            Ok(Box::new(NetworkTransport::new_with_tls_mode(
                base_stream,
                SslHandler {
                    server_host_name: transport_context.get_server_name().to_string(),
                    encryption_options,
                },
                PRE_NEGOTIATED_PACKET_SIZE,
                encryption_mode,
                true, // Use TDS 7.4 TLS wrapping
            )))
        }
        TdsVersion::V8_0 => {
            // Enable TLS over TCP immediately in TDS 8.0
            let ssl_handler = SslHandler {
                server_host_name: transport_context.get_server_name().to_string(),
                encryption_options,
            };
            let encrypted_stream = ssl_handler.enable_ssl_async(Box::new(stream)).await?;

            Ok(Box::new(NetworkTransport::new(
                encrypted_stream,
                ssl_handler,
                PRE_NEGOTIATED_PACKET_SIZE,
                encryption_mode,
            )))
        }
        TdsVersion::Unknown(version_value) => Err(crate::error::Error::ProtocolError(format!(
            "Unsupported TDS version: 0x{version_value:08X}. Only TDS 7.4 and TDS 8.0 are supported."
        ))),
    }
}

#[async_trait]
pub trait TransportSslHandler {
    async fn enable_ssl(&mut self) -> TdsResult<()>;
    async fn disable_ssl(&mut self) -> TdsResult<()>;
}

pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send + Sync {
    fn tls_handshake_starting(&mut self);
    fn tls_handshake_completed(&mut self);
}

impl Stream for TcpStream {
    fn tls_handshake_starting(&mut self) {
        // No-op for plain TCP streams
    }

    fn tls_handshake_completed(&mut self) {
        // No-op for plain TCP streams
    }
}

impl Stream for Box<dyn Stream> {
    fn tls_handshake_starting(&mut self) {
        (**self).tls_handshake_starting();
    }

    fn tls_handshake_completed(&mut self) {
        (**self).tls_handshake_completed();
    }
}

pub(crate) struct NetworkTransport {
    encryption: Option<NegotiatedEncryptionSetting>,
    packet_size: u32,
    stream: Option<Box<dyn Stream>>,
    ssl_handler: SslHandler,
    encryption_setting: EncryptionSetting,
    tds_read_buffer: TdsReadBuffer,
    use_tds74_tls_wrapping: bool,
}

impl std::fmt::Debug for NetworkTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkTransport")
            .field("encryption", &self.encryption)
            .field("packet_size", &self.packet_size)
            .field("stream", &"<stream>")
            .field("ssl_handler", &self.ssl_handler)
            .field("encryption_setting", &self.encryption_setting)
            .finish()
    }
}

impl NetworkReaderWriter for NetworkTransport {
    fn notify_encryption_setting_change(&mut self, setting: NegotiatedEncryptionSetting) {
        self.notify_encryption_negotiation(setting);
    }

    fn notify_session_setting_change(&mut self, setting: &SessionSettings) {
        self.packet_size = setting.packet_size;
    }

    fn as_writer(&mut self) -> &mut dyn NetworkWriter {
        self
    }
}

#[async_trait]
impl NetworkReader for NetworkTransport {
    async fn receive(&mut self, buffer: &mut [u8]) -> TdsResult<usize> {
        Ok(self.receive(buffer).await?)
    }

    fn packet_size(&self) -> u32 {
        self.packet_size
    }
}

#[async_trait]
impl NetworkWriter for NetworkTransport {
    async fn send(&mut self, data: &[u8]) -> TdsResult<()> {
        self.stream
            .as_mut()
            .expect("Stream not available")
            .write_all(data)
            .await?;
        Ok(())
    }

    fn packet_size(&self) -> u32 {
        self.packet_size
    }

    fn get_encryption_setting(&self) -> NegotiatedEncryptionSetting {
        assert!(self.encryption.is_some());
        self.encryption.unwrap()
    }
}

impl NetworkTransport {
    pub fn new(
        stream: Box<dyn Stream>,
        ssl_handler: SslHandler,
        packet_size: u32,
        encryption_setting: EncryptionSetting,
    ) -> Self {
        Self::new_with_tls_mode(stream, ssl_handler, packet_size, encryption_setting, false)
    }

    pub fn new_with_tls_mode(
        stream: Box<dyn Stream>,
        ssl_handler: SslHandler,
        packet_size: u32,
        encryption_setting: EncryptionSetting,
        use_tds74_tls_wrapping: bool,
    ) -> Self {
        Self {
            encryption: None,
            stream: Some(stream),
            ssl_handler,
            packet_size,
            encryption_setting,
            tds_read_buffer: TdsReadBuffer::new(packet_size as usize),
            use_tds74_tls_wrapping,
        }
    }

    pub(crate) async fn send(&mut self, data: &[u8]) -> TdsResult<()> {
        self.stream
            .as_mut()
            .expect("Stream not available")
            .write_all(data)
            .await?;
        Ok(())
    }

    pub(crate) fn notify_encryption_negotiation(
        &mut self,
        encryption: NegotiatedEncryptionSetting,
    ) {
        assert!(self.encryption.is_none());
        self.encryption = Some(encryption);
    }

    pub(crate) async fn receive(&mut self, buffer: &mut [u8]) -> TdsResult<usize> {
        if buffer.is_empty() {
            return Err(crate::error::Error::UsageError(
                "Buffer length must be greater than 0".to_string(),
            ));
        }
        let bytes_read = self
            .stream
            .as_mut()
            .expect("Stream not available")
            .read(buffer)
            .await?;
        if bytes_read == 0 {
            Err(crate::error::Error::from(std::io::Error::from(
                UnexpectedEof,
            )))
        } else {
            Ok(bytes_read)
        }
    }

    async fn enable_ssl_internal(&mut self) -> TdsResult<()> {
        // Take ownership of the stream temporarily
        let base_stream = self.stream.take().expect("Stream already taken");

        // For TDS 7.4, wrap the stream in TlsOverTdsStream before TLS handshake
        // This is required because TLS packets must be framed within TDS packets during the handshake
        let base_stream = if self.use_tds74_tls_wrapping {
            Box::new(crate::connection::transport::ssl_handler::TlsOverTdsStream::new(base_stream))
        } else {
            base_stream
        };

        // Perform TLS handshake (consumes base_stream, returns TlsStream)
        // enable_ssl_async will call tls_handshake_starting and tls_handshake_completed internally
        let encrypted_stream = self.ssl_handler.enable_ssl_async(base_stream).await?;

        // Put back the encrypted stream
        self.stream = Some(encrypted_stream);
        Ok(())
    }

    async fn disable_ssl_internal(&mut self) {
        // Take the current stream (which should be encrypted)
        let _encrypted_stream = self.stream.take().expect("Stream not available");

        // For disable_ssl, we would need to extract the base stream from the TLS wrapper
        // This is not currently supported in the architecture, so this is a placeholder
        // In practice, disabling SSL mid-connection is rare
        panic!("Disabling SSL is not supported in the simplified stream model");
    }

    pub(crate) async fn close_transport(&mut self) -> TdsResult<()> {
        if let Some(stream) = self.stream.as_mut() {
            stream.shutdown().await?;
        }
        Ok(())
    }

    async fn read_tds_packet(&mut self) -> TdsResult<()> {
        // let remaining_bytes = self.buffer_length - self.buffer_position;
        let remaining_bytes = self.tds_read_buffer.get_remaining_byte_count();
        if remaining_bytes > 0 {
            // Move the remaining bytes to the beginning of the buffer.
            self.tds_read_buffer.shift_data_to_front();
            let new_packet_size = self.get_new_tds_packet().await?;
            self.tds_read_buffer
                .remove_header_from_packet(new_packet_size);
        } else {
            self.tds_read_buffer.reset_to_length(0);
            let new_packet_size = self.get_new_tds_packet().await?;
            self.tds_read_buffer
                .remove_header_from_packet(new_packet_size);
        }
        Ok(())
    }

    async fn get_new_tds_packet(&mut self) -> TdsResult<usize> {
        let base_offset = self.tds_read_buffer.buffer_length;
        let max_packet_size = self.tds_read_buffer.max_packet_size;

        let stream = self.stream.as_mut().expect("Stream not available");
        let mut bytes_read_from_transport = stream
            .read(&mut self.tds_read_buffer.working_buffer[base_offset..])
            .await?;

        // We need the 8 byte header. Re-read, in case the new_packet_byte_length has less bytes than 8 bytes to complete
        // the header.
        while bytes_read_from_transport < PacketWriter::PACKET_HEADER_SIZE {
            bytes_read_from_transport += stream
                .read(
                    &mut self.tds_read_buffer.working_buffer
                        [base_offset + bytes_read_from_transport..base_offset + max_packet_size],
                )
                .await?;
        }

        let length_from_packet_header = BigEndian::read_u16(
            &self.tds_read_buffer.working_buffer[base_offset + 2..base_offset + 4],
        );

        let packet_size_from_header: usize = length_from_packet_header as usize;

        // Keep reading until we have the complete packet in memory.
        while bytes_read_from_transport < packet_size_from_header {
            bytes_read_from_transport += stream
                .read(
                    &mut self.tds_read_buffer.working_buffer
                        [base_offset + bytes_read_from_transport..base_offset + max_packet_size],
                )
                .await?;
        }
        event!(
            tracing::Level::DEBUG,
            "Received packet of size: {:?}",
            bytes_read_from_transport
        );

        use pretty_hex::PrettyHex;

        event!(
            tracing::Level::DEBUG,
            "Packet content: {:?}",
            &mut self.tds_read_buffer.working_buffer
                [base_offset..base_offset + bytes_read_from_transport]
                .hex_dump()
        );
        Ok(bytes_read_from_transport)
    }

    async fn receive_token_internal(&mut self, context: &ParserContext) -> TdsResult<Tokens> {
        // Read the token type so that we can get the right parser for this token.
        // The first byte of the token is the token type.
        let token_type_byte = self.read_byte().await?;
        let token_type: TokenType = token_type_byte.try_into()?;
        debug!(
            "Received token type: {:?} ({})",
            token_type, token_type_byte
        );

        // We should always have a parser for the token type.
        // If we don't, then we have a bug in the code.
        if !PARSER_REGISTRY.has_parser(&token_type) {
            return Err(crate::error::Error::ImplementationError(format!(
                "No parser registered for token type: {token_type:?}"
            )));
        }

        let parser = PARSER_REGISTRY
            .get_parser(&token_type)
            .expect("Parser not found");

        event!(
            tracing::Level::DEBUG,
            "Parsing token type: {:?}",
            &token_type
        );

        match parser {
            TokenParsers::EnvChange(parser) => parser.parse(self, context).await,
            TokenParsers::LoginAck(parser) => parser.parse(self, context).await,
            TokenParsers::Done(parser) => parser.parse(self, context).await,
            TokenParsers::DoneInProc(parser) => parser.parse(self, context).await,
            TokenParsers::DoneProc(parser) => parser.parse(self, context).await,
            TokenParsers::Info(parser) => parser.parse(self, context).await,
            TokenParsers::Error(parser) => parser.parse(self, context).await,
            TokenParsers::FedAuthInfo(parser) => parser.parse(self, context).await,
            TokenParsers::FeatureExtAck(parser) => parser.parse(self, context).await,
            TokenParsers::ColMetadata(parser) => parser.parse(self, context).await,
            TokenParsers::Row(parser) => parser.parse(self, context).await,
            TokenParsers::Order(parser) => parser.parse(self, context).await,
            TokenParsers::ReturnStatus(parser) => parser.parse(self, context).await,
            TokenParsers::NbcRow(parser) => parser.parse(self, context).await,
            TokenParsers::ReturnValue(parser) => parser.parse(self, context).await,
        }
    }

    /// Tells the server to stop sending tokens for the token stream being read and waits for
    /// an acknowledgement.
    async fn cancel_read_stream_and_wait(&mut self) -> TdsResult<()> {
        self.cancel_read_stream().await?;
        let dummy_context = ParserContext::None(());
        // This method is intended to be called from receive_token(). We enforce only one level
        // of recursion by preventing timeout and cancellation on the internal receive_token() call.
        while let Ok(token) = self.receive_token_internal(&dummy_context).await {
            if let Tokens::Done(done_token) = token {
                if done_token.status.contains(DoneStatus::ATTN) {
                    break;
                }
                // Discard any other token.
            }
        }
        Ok(())
    }
}

#[async_trait]
impl TransportSslHandler for NetworkTransport {
    async fn enable_ssl(&mut self) -> TdsResult<()> {
        self.enable_ssl_internal().await
    }

    async fn disable_ssl(&mut self) -> TdsResult<()> {
        let encryption_type_check = match self.encryption_setting {
            EncryptionSetting::Strict => {
                // TODO: Evaluate this error.
                Err(crate::error::Error::from(Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Under strict mode the client must communicate over TLS",
                )))
            }
            _ => Ok(()),
        };

        if encryption_type_check.is_err() {
            encryption_type_check
        } else {
            self.disable_ssl_internal().await;
            Ok(())
        }
    }
}

#[async_trait]
impl TdsPacketReader for NetworkTransport {
    fn reset_reader(&mut self) {
        // Make sure that we have read all the data from the buffer.
        assert!(self.tds_read_buffer.buffer_length == self.tds_read_buffer.buffer_position);
        self.tds_read_buffer
            .change_packet_size(NetworkReader::packet_size(self));
        self.tds_read_buffer.reset_to_length(0);
    }

    async fn read_byte(&mut self) -> TdsResult<u8> {
        if !self.tds_read_buffer.do_we_have_enough_data(1) {
            self.read_tds_packet().await?;
        }
        let result: u8 = self.tds_read_buffer.working_buffer[self.tds_read_buffer.buffer_position];
        self.tds_read_buffer.consume_bytes(1);
        Ok(result)
    }

    async fn read_int16_big_endian(&mut self) -> TdsResult<i16> {
        if !self.tds_read_buffer.do_we_have_enough_data(2) {
            self.read_tds_packet().await?;
        }
        let result = BigEndian::read_i16(self.tds_read_buffer.get_slice());
        self.tds_read_buffer.consume_bytes(2);
        Ok(result)
    }
    async fn read_int32_big_endian(&mut self) -> TdsResult<i32> {
        if !self.tds_read_buffer.do_we_have_enough_data(4) {
            self.read_tds_packet().await?;
        }
        let result = BigEndian::read_i32(self.tds_read_buffer.get_slice());
        self.tds_read_buffer.consume_bytes(4);
        Ok(result)
    }

    async fn read_int64_big_endian(&mut self) -> TdsResult<i64> {
        if !self.tds_read_buffer.do_we_have_enough_data(8) {
            self.read_tds_packet().await?;
        }
        let result = BigEndian::read_i64(self.tds_read_buffer.get_slice());
        self.tds_read_buffer.consume_bytes(8);
        Ok(result)
    }

    async fn read_uint40(&mut self) -> TdsResult<u64> {
        if !self.tds_read_buffer.do_we_have_enough_data(5) {
            self.read_tds_packet().await?;
        }

        let result = LittleEndian::read_uint(self.tds_read_buffer.get_slice(), 5);
        self.tds_read_buffer.consume_bytes(5);
        Ok(result)
    }

    async fn read_float32(&mut self) -> TdsResult<f32> {
        if !self.tds_read_buffer.do_we_have_enough_data(4) {
            self.read_tds_packet().await?;
        }
        let result = LittleEndian::read_f32(self.tds_read_buffer.get_slice());
        self.tds_read_buffer.consume_bytes(4);
        Ok(result)
    }
    async fn read_float64(&mut self) -> TdsResult<f64> {
        if !self.tds_read_buffer.do_we_have_enough_data(8) {
            self.read_tds_packet().await?;
        }
        let result = LittleEndian::read_f64(self.tds_read_buffer.get_slice());
        self.tds_read_buffer.consume_bytes(8);
        Ok(result)
    }
    async fn read_int16(&mut self) -> TdsResult<i16> {
        if !self.tds_read_buffer.do_we_have_enough_data(2) {
            self.read_tds_packet().await?;
        }
        let result = LittleEndian::read_i16(self.tds_read_buffer.get_slice());
        self.tds_read_buffer.consume_bytes(2);
        Ok(result)
    }
    async fn read_uint16(&mut self) -> TdsResult<u16> {
        if !self.tds_read_buffer.do_we_have_enough_data(2) {
            self.read_tds_packet().await?;
        }
        let result = LittleEndian::read_u16(self.tds_read_buffer.get_slice());
        self.tds_read_buffer.consume_bytes(2);
        Ok(result)
    }
    async fn read_int24(&mut self) -> TdsResult<i32> {
        if !self.tds_read_buffer.do_we_have_enough_data(3) {
            self.read_tds_packet().await?;
        }
        let result = LittleEndian::read_i24(self.tds_read_buffer.get_slice());
        self.tds_read_buffer.consume_bytes(3);
        Ok(result)
    }
    async fn read_uint24(&mut self) -> TdsResult<u32> {
        if !self.tds_read_buffer.do_we_have_enough_data(3) {
            self.read_tds_packet().await?;
        }
        let result = LittleEndian::read_u24(self.tds_read_buffer.get_slice());
        self.tds_read_buffer.consume_bytes(3);
        Ok(result)
    }

    async fn read_int32(&mut self) -> TdsResult<i32> {
        if !self.tds_read_buffer.do_we_have_enough_data(4) {
            self.read_tds_packet().await?;
        }
        let result = LittleEndian::read_i32(self.tds_read_buffer.get_slice());
        self.tds_read_buffer.consume_bytes(4);
        Ok(result)
    }

    async fn read_uint32(&mut self) -> TdsResult<u32> {
        if !self.tds_read_buffer.do_we_have_enough_data(4) {
            self.read_tds_packet().await?;
        }
        let result = LittleEndian::read_u32(self.tds_read_buffer.get_slice());
        self.tds_read_buffer.consume_bytes(4);
        Ok(result)
    }
    async fn read_int64(&mut self) -> TdsResult<i64> {
        if !self.tds_read_buffer.do_we_have_enough_data(8) {
            self.read_tds_packet().await?;
        }
        let result = LittleEndian::read_i64(self.tds_read_buffer.get_slice());
        self.tds_read_buffer.consume_bytes(8);
        Ok(result)
    }
    async fn read_uint64(&mut self) -> TdsResult<u64> {
        if !self.tds_read_buffer.do_we_have_enough_data(8) {
            self.read_tds_packet().await?;
        }
        let result = LittleEndian::read_u64(self.tds_read_buffer.get_slice());
        self.tds_read_buffer.consume_bytes(8);
        Ok(result)
    }

    async fn read_bytes(&mut self, buffer: &mut [u8]) -> TdsResult<usize> {
        let mut total_read = 0;
        let mut length_to_read = buffer.len();
        let mut offset = 0;
        while length_to_read > 0 {
            if !self
                .tds_read_buffer
                .do_we_have_enough_data(min(self.tds_read_buffer.max_packet_size, length_to_read))
            {
                self.read_tds_packet().await?;
            }
            let available = self.tds_read_buffer.get_remaining_byte_count();

            // We can read the minimum of what is available, or the actual length needed or the packet size.
            let to_read = min(
                available,
                min(length_to_read, self.tds_read_buffer.max_packet_size - 8),
            );

            if to_read > 0 {
                // Copy from self.working_buffer to buffer from self.buffer_position to offset.
                buffer[offset..offset + to_read].copy_from_slice(
                    &self.tds_read_buffer.working_buffer[self.tds_read_buffer.buffer_position
                        ..self.tds_read_buffer.buffer_position + to_read],
                );
                offset += to_read;
                length_to_read -= to_read;
                total_read += to_read;

                self.tds_read_buffer.consume_bytes(to_read);
            }
        }
        Ok(total_read)
    }

    async fn read_u8_varbyte(&mut self) -> TdsResult<Vec<u8>> {
        let length: u8 = self.read_byte().await?;
        let mut result: Vec<u8> = vec![0; length as usize];
        self.read_bytes(&mut result[0..]).await?;
        Ok(result)
    }

    async fn read_u16_varbyte(&mut self) -> TdsResult<Vec<u8>> {
        let length: u16 = self.read_uint16().await?;
        let mut result: Vec<u8> = vec![0; length as usize];
        self.read_bytes(&mut result[0..]).await?;
        Ok(result)
    }

    async fn read_varchar_u16_length(&mut self) -> TdsResult<Option<String>> {
        let length: u16 = self.read_uint16().await?;
        if length == PacketReader::LENGTHNULL {
            return Ok(None);
        }

        let string = self
            .read_unicode_with_byte_length((length << 1) as usize)
            .await?;
        Ok(Some(string))
    }

    async fn read_varchar_u8_length(&mut self) -> TdsResult<String> {
        let length: u8 = self.read_byte().await?;
        let string = self
            .read_unicode_with_byte_length((length << 1) as usize)
            .await?;
        Ok(string)
    }
    async fn read_varchar_byte_len(&mut self) -> TdsResult<String> {
        let length: u16 = self.read_uint16().await?;
        let string = self.read_unicode_with_byte_length(length as usize).await?;
        Ok(string)
    }
    async fn read_unicode(&mut self, string_length: usize) -> TdsResult<String> {
        let result = self
            .read_unicode_with_byte_length(string_length * 2)
            .await?;
        Ok(result)
    }
    async fn read_unicode_with_byte_length(&mut self, byte_length: usize) -> TdsResult<String> {
        let mut byte_buffer: Vec<u8> = vec![0; byte_length];
        let _ = self.read_bytes(&mut byte_buffer[0..]).await?;

        // TODO: This smells like a performance problem. We are copy from a u8 vector to u16.
        // We will revisit this and fix it. Needs some rust research.
        let mut u16_buffer = Vec::with_capacity(byte_buffer.len() / 2);
        for chunk in byte_buffer.chunks(2) {
            let value = u16::from_le_bytes([chunk[0], chunk[1]]);
            u16_buffer.push(value);
        }
        // Convert byte_buffer to a unicode string
        let string =
            String::from_utf16(&u16_buffer).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        Ok(string)
    }

    async fn skip_bytes(&mut self, skip_count: usize) -> TdsResult<()> {
        let mut length_to_read = skip_count;
        while length_to_read > 0 {
            if !self.tds_read_buffer.do_we_have_enough_data(min(
                self.tds_read_buffer.max_packet_size - 8,
                length_to_read,
            )) {
                self.read_tds_packet().await?;
            }
            let available = self.tds_read_buffer.get_remaining_byte_count();

            // We can read the minimum of what is available, or the actual length needed or the packet size.
            let to_read = min(
                available,
                min(length_to_read, self.tds_read_buffer.max_packet_size - 8),
            );

            if to_read > 0 {
                length_to_read -= to_read;
                self.tds_read_buffer.consume_bytes(to_read);
            }
        }
        Ok(())
    }

    async fn cancel_read_stream(&mut self) -> TdsResult<()> {
        let attention = AttentionRequest::new();
        let mut packet_writer = attention.create_packet_writer(self.as_writer(), None, None);
        attention.serialize(&mut packet_writer).await?;
        Ok(())
    }
}

#[async_trait]
impl TdsTokenStreamReader for NetworkTransport {
    async fn receive_token(
        &mut self,
        context: &ParserContext,
        remaining_request_timeout: Option<Duration>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<Tokens> {
        let cancellable_receive_token =
            CancelHandle::run_until_cancelled(cancel_handle, self.receive_token_internal(context));
        let token_result = match remaining_request_timeout.as_ref() {
            Some(remaining_request_timeout) => {
                match timeout(*remaining_request_timeout, cancellable_receive_token).await {
                    Ok(result) => result,
                    Err(elapsed) => Err(TimeoutError(TimeoutErrorType::Elapsed(elapsed))),
                }
            }
            None => cancellable_receive_token.await,
        };

        match &token_result {
            Ok(_) => {}
            Err(err) => match err {
                OperationCancelledError(_) | TimeoutError(_) => {
                    self.cancel_read_stream_and_wait().await?;
                }
                _ => {}
            },
        }
        token_result
    }
}

#[async_trait]
impl crate::connection::transport::tds_transport::TdsTransport for NetworkTransport {
    fn as_writer(&mut self) -> &mut dyn NetworkWriter {
        self
    }

    fn reset_reader(&mut self) {
        self.tds_read_buffer.reset_to_length(0);
    }

    fn packet_size(&self) -> u32 {
        self.packet_size
    }

    async fn close_transport(&mut self) -> TdsResult<()> {
        if let Some(stream) = self.stream.as_mut() {
            stream.shutdown().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*; // Brings in NetworkTransport, SslHandler, StreamRecoverer, etc.
    use crate::connection::client_context::ClientContext;
    use crate::connection::transport::network_transport::Stream;
    use crate::connection::transport::ssl_handler::SslHandler;
    use crate::core::EncryptionOptions;
    use bytes::Bytes;
    use futures::SinkExt;
    use futures::StreamExt;
    use rand::Rng;
    use tokio::io::{DuplexStream, duplex};
    use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

    // The choice of 8192 is large enough for sending data. This stream should have a buffer large enough for send.
    // The test would keep the payload lower than this size to make sure that the duplex stream can handle it.
    pub(crate) const MAX_BUFFER_SIZE: usize = 8192;

    /// A mock SslHandler that simply returns the same stream, no real TLS.
    pub(crate) struct MockSslHandler;

    impl Stream for DuplexStream {
        fn tls_handshake_starting(&mut self) {
            // No-op for duplex streams
        }

        fn tls_handshake_completed(&mut self) {
            // No-op for duplex streams
        }
    }

    pub(crate) fn create_readable_network_transport(
        context: &ClientContext,
    ) -> (NetworkTransport, DuplexStream) {
        let (client_side, server_side) = duplex(MAX_BUFFER_SIZE);

        let ssl_handler = SslHandler {
            server_host_name: context.transport_context.get_server_name().clone(),
            encryption_options: context.encryption_options.clone(),
        };

        (
            NetworkTransport::new(
                Box::new(client_side),
                ssl_handler,
                context.packet_size as u32,
                context.encryption_options.mode,
            ),
            server_side,
        )
    }

    pub(crate) fn create_client_server_network_transport(
        context: &ClientContext,
    ) -> (NetworkTransport, NetworkTransport) {
        let (client_side, server_side) = duplex(MAX_BUFFER_SIZE);

        let ssl_handler = SslHandler {
            server_host_name: context.transport_context.get_server_name().clone(),
            encryption_options: context.encryption_options.clone(),
        };

        (
            NetworkTransport::new(
                Box::new(client_side),
                ssl_handler,
                context.packet_size as u32,
                context.encryption_options.mode,
            ),
            NetworkTransport::new(
                Box::new(server_side),
                SslHandler {
                    server_host_name: context.transport_context.get_server_name().clone(),
                    encryption_options: context.encryption_options.clone(),
                },
                context.packet_size as u32,
                context.encryption_options.mode,
            ),
        )
    }

    #[tokio::test]
    async fn test_network_transport_send() {
        let context = ClientContext {
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::On,
                trust_server_certificate: true,
                ..EncryptionOptions::default()
            },
            ..Default::default()
        };
        let (mut transport, server_side) = create_readable_network_transport(&context);

        // Fill data_to_send with random values
        let mut rng = rand::rng();
        let data_vector: Vec<u8> = (0..MAX_BUFFER_SIZE).map(|_| rng.random()).collect();

        // Setup the reader to read the data.
        let mut framed_reader = FramedRead::new(server_side, BytesCodec::new());

        // Send the data and read it from the other end of the pipe.
        let result = transport.send(&data_vector[..]).await;
        match result {
            Ok(_) => {}
            Err(e) => panic!("Error sending data: {e}"),
        }

        let received = framed_reader
            .next()
            .await
            .expect("No data")
            .expect("Decode error");

        assert_eq!(received.as_ref(), &data_vector[..]);
    }

    /// A basic test showing that `receive` can read data from the transport's reader.
    #[tokio::test]
    async fn test_network_transport_receive() -> TdsResult<()> {
        // 1) Create an in-memory duplex stream (client_side, server_side).
        // Data will be written on the server_side and the network transport will read from the client side.
        let (client_side, server_side) = duplex(1024);

        // Mocks and defaults.
        let context = ClientContext {
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::On,
                trust_server_certificate: true,
                ..EncryptionOptions::default()
            },
            ..Default::default()
        };
        let ssl_handler = SslHandler {
            server_host_name: context.transport_context.get_server_name().clone(),
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::On,
                trust_server_certificate: true,
                ..EncryptionOptions::default()
            },
        };

        // Optionally, shut down the writer so the reader sees EOF if all data is read
        // client_writer.shutdown().await?;

        // Build our transport
        let mut transport = NetworkTransport::new(
            Box::new(client_side),
            ssl_handler,
            context.packet_size as u32,
            context.encryption_options.mode,
        );

        let mut rng = rand::rng();
        let data_size = 128;
        let data_written: Vec<u8> = (0..data_size).map(|_| rng.random()).collect();
        let mut framed_writer = FramedWrite::new(server_side, BytesCodec::new());
        framed_writer
            .send(Bytes::copy_from_slice(&data_written[..]))
            .await?;

        // 5) Attempt to read from the transport into a buffer
        let mut buffer = vec![0u8; data_size];
        let bytes_read = transport.receive(&mut buffer).await?;

        // Verify we read exactly `data_size` bytes and that they match what was written
        assert_eq!(bytes_read, data_size);
        assert_eq!(buffer, data_written);

        Ok(())
    }
}
