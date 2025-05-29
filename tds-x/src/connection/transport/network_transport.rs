use crate::connection::client_context::{ClientContext, IPAddressPreference, TransportContext};
use crate::connection::transport::ssl_handler::{SslHandler, Tds7StreamRecoverer};
use crate::core::{EncryptionSetting, NegotiatedEncryptionSetting, TdsResult};
use crate::handler::handler_factory::SessionSettings;
use crate::message::login_options::TdsVersion;
use crate::read_write::packet_reader::PacketReader;
use crate::read_write::reader_writer::{NetworkReader, NetworkReaderWriter, NetworkWriter};
use async_trait::async_trait;
use futures::lock::Mutex;
use std::io::Error;
use std::io::ErrorKind::UnexpectedEof;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{self, TcpStream};
use tracing::{info, trace};

pub(crate) const PRE_NEGOTIATED_PACKET_SIZE: u32 = 4096;

pub(crate) async fn create_transport<'a>(
    context: &'a ClientContext,
    transport_context: &TransportContext,
) -> TdsResult<Box<NetworkTransport<'a>>> {
    let stream = match &transport_context {
        TransportContext::Tcp { host, port } => {
            info!("Connecting to TCP transport: {}:{}", host, port);

            // This will cause the DNS resolution of the addresses.
            let mut socket_addresses = (host.as_str(), *port).to_socket_addrs()?;

            let mut last_error = None;
            let mut tcp_stream = None;

            // Sort the address list based on the IP address preference
            match context.ipaddress_preference {
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
        _ => unimplemented!("Only TCP transport is supported"),
    };

    // Convert the Tokio stream to a std::TcpStream to make it easier to clone.
    // Do this outside the callback instead of making the Tokio Stream part of the closure
    // because into_std() is a destructive operation.
    let std_stream = stream.into_std()?;

    // Define a cloning callback since this is the only time we know we're working with
    // TcpStreams.

    match context.tds_version() {
        TdsVersion::V7_4 => {
            let stream_recoverer = Tds7StreamRecoverer::new(TcpStreamRecoverer {
                stream: Box::new(std_stream),
            });

            // TDS 7.4 starts with unencrypted streams that could get encrypted as part of prelogin
            // negotiation.
            let base_stream = stream_recoverer.recover_base_stream();

            Ok(Box::new(NetworkTransport {
                context,
                encryption: None,
                stream: Arc::new(Mutex::new(base_stream)),
                ssl_handler: SslHandler {
                    server_host_name: transport_context.get_server_name().to_string(),
                    encryption_options: context.encryption_options.clone(),
                },
                stream_recoverer: Box::new(stream_recoverer),
                packet_size: PRE_NEGOTIATED_PACKET_SIZE,
            }))
        }
        TdsVersion::V8_0 => {
            let stream_recoverer = TcpStreamRecoverer {
                stream: Box::new(std_stream),
            };

            // Enable TLS over TCP immediately in TDS 8.0
            let ssl_handler = SslHandler {
                server_host_name: transport_context.get_server_name().to_string(),
                encryption_options: context.encryption_options.clone(),
            };
            let encrypted_stream = ssl_handler
                .enable_ssl_async(stream_recoverer.recover_base_stream())
                .await?;

            Ok(Box::new(NetworkTransport {
                context,
                encryption: None,
                stream: Arc::new(Mutex::new(encrypted_stream)),
                ssl_handler,
                stream_recoverer: Box::new(stream_recoverer),
                packet_size: PRE_NEGOTIATED_PACKET_SIZE,
            }))
        }
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

pub(crate) trait StreamRecoverer: Send + Sync {
    fn recover_base_stream(&self) -> Box<dyn Stream>;
    fn tls_handshake_starting(&mut self);
    fn tls_handshake_completed(&mut self);
}

impl Stream for TcpStream {
    fn tls_handshake_starting(&mut self) {}
    fn tls_handshake_completed(&mut self) {}
}

struct TcpStreamRecoverer {
    pub stream: Box<std::net::TcpStream>,
}

impl StreamRecoverer for TcpStreamRecoverer {
    fn recover_base_stream(&self) -> Box<dyn Stream> {
        let std_stream_clone = self.stream.try_clone().unwrap();
        let tokio_stream = TcpStream::from_std(std_stream_clone).unwrap();
        Box::new(tokio_stream)
    }

    fn tls_handshake_starting(&mut self) {}

    fn tls_handshake_completed(&mut self) {}
}

pub(crate) struct NetworkTransport<'a> {
    context: &'a ClientContext,
    encryption: Option<NegotiatedEncryptionSetting>,
    packet_size: u32,
    stream: Arc<Mutex<dyn Stream>>,
    ssl_handler: SslHandler,
    stream_recoverer: Box<dyn StreamRecoverer + 'a>,
}

impl NetworkReaderWriter for NetworkTransport<'_> {
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
impl NetworkReader for NetworkTransport<'_> {
    async fn receive(&mut self, buffer: &mut [u8]) -> TdsResult<usize> {
        Ok(self.receive(buffer).await?)
    }

    fn packet_size(&self) -> u32 {
        self.packet_size
    }

    fn get_packet_reader(&mut self) -> PacketReader<'_> {
        PacketReader::new(self)
    }
}

#[async_trait]
impl NetworkWriter for NetworkTransport<'_> {
    async fn send(&mut self, data: &[u8]) -> TdsResult<()> {
        self.stream.lock().await.write_all(data).await?;
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

impl NetworkTransport<'_> {
    pub(crate) async fn send(&mut self, data: &[u8]) -> TdsResult<()> {
        self.stream.lock().await.write_all(data).await?;
        Ok(())
    }

    pub(crate) fn notify_encryption_negotiation(
        &mut self,
        encryption: NegotiatedEncryptionSetting,
    ) {
        assert!(self.encryption.is_none());
        self.encryption = Some(encryption);
    }

    /// Asynchronously reads data from the underlying network transport
    /// into the caller-provided buffer.
    ///
    /// # Arguments
    ///
    /// * `buffer` - The mutable slice to store the data read from the stream.
    ///
    /// # Returns
    ///
    /// Returns the number of bytes actually read, or an [`tds_x::error::Error`] if
    /// something goes wrong. Reading fewer bytes than `buffer.len()` is common
    /// (especially if only part of the data is available).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # use std::io::Error;
    /// # use tokio::io::{AsyncReadExt, split};
    /// # use tokio_util::codec::BytesCodec;
    /// #
    /// # async fn demo(mut transport: NetworkTransport<'_>) -> TdsResult<()> {
    ///     let mut buf = [0u8; 1024];
    ///     let bytes_read = transport.receive(&mut buf).await?;
    ///     println!("Read {} bytes", bytes_read);
    ///     Ok(())
    /// # }
    /// ```
    pub(crate) async fn receive(&mut self, buffer: &mut [u8]) -> TdsResult<usize> {
        if buffer.is_empty() {
            panic!("Buffer length must be greater than 0");
        }
        let bytes_read = self.stream.lock().await.read(buffer).await?;
        if bytes_read == 0 {
            Err(crate::error::Error::from(std::io::Error::from(
                UnexpectedEof,
            )))
        } else {
            Ok(bytes_read)
        }
    }

    async fn enable_ssl_internal(&mut self) -> TdsResult<()> {
        self.stream_recoverer.tls_handshake_starting();
        let encrypted_stream = self
            .ssl_handler
            .enable_ssl_async(self.stream_recoverer.recover_base_stream())
            .await?;

        self.stream_recoverer.tls_handshake_completed();
        self.stream = Arc::new(Mutex::new(encrypted_stream));
        Ok(())
    }

    async fn disable_ssl_internal(&mut self) {
        let base_stream = self.stream_recoverer.recover_base_stream();
        self.stream = Arc::new(Mutex::new(base_stream));
    }

    pub(crate) async fn close_transport(&mut self) -> TdsResult<()> {
        self.stream.lock().await.shutdown().await?;
        Ok(())
    }
}

#[async_trait]
impl TransportSslHandler for NetworkTransport<'_> {
    async fn enable_ssl(&mut self) -> TdsResult<()> {
        self.enable_ssl_internal().await
    }

    async fn disable_ssl(&mut self) -> TdsResult<()> {
        let encryption_type_check = match self.context.encryption_options.mode {
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*; // Brings in NetworkTransport, SslHandler, StreamRecoverer, etc.
    use crate::connection::client_context::ClientContext;
    use crate::connection::transport::network_transport::Stream; // Your custom trait
    use crate::connection::transport::ssl_handler::SslHandler;
    use crate::core::EncryptionOptions;
    use bytes::Bytes;
    use futures::SinkExt;
    use futures::StreamExt;
    use rand::Rng;
    use tokio::io::{duplex, DuplexStream};
    use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

    // The choice of 8192 is large enough for sending data. This stream should have a buffer large enough for send.
    // The test would keep the payload lower than this size to make sure that the duplex stream can handle it.
    pub(crate) const MAX_BUFFER_SIZE: usize = 8192;

    /// A mock SslHandler that simply returns the same stream, no real TLS.
    pub(crate) struct MockSslHandler;

    /// A mock StreamRecoverer that always returns the stored DuplexStream.
    pub(crate) struct MockStreamRecoverer {}

    impl Stream for DuplexStream {
        fn tls_handshake_starting(&mut self) {}
        fn tls_handshake_completed(&mut self) {}
    }

    impl StreamRecoverer for MockStreamRecoverer {
        fn recover_base_stream(&self) -> Box<dyn Stream> {
            // Take the DuplexStream out of the option, consuming it.
            let (dummy_client, _) = duplex(1);
            let stream = dummy_client;
            Box::new(stream)
        }

        fn tls_handshake_starting(&mut self) {}
        fn tls_handshake_completed(&mut self) {}
    }

    pub(crate) fn create_readable_network_transport(
        context: &ClientContext,
    ) -> (NetworkTransport, DuplexStream) {
        let (client_side, server_side) = duplex(MAX_BUFFER_SIZE);

        let ssl_handler = SslHandler {
            server_host_name: context.transport_context.get_server_name().clone(),
            encryption_options: context.encryption_options.clone(),
        };
        let stream_recoverer = Box::new(MockStreamRecoverer {});

        (
            NetworkTransport {
                context,
                encryption: None,
                stream: Arc::new(Mutex::new(client_side)),
                ssl_handler,
                stream_recoverer,
                packet_size: context.packet_size as u32,
            },
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
        let stream_recoverer = Box::new(MockStreamRecoverer {});

        (
            NetworkTransport {
                context,
                encryption: None,
                stream: Arc::new(Mutex::new(client_side)),
                ssl_handler,
                stream_recoverer,
                packet_size: context.packet_size as u32,
            },
            NetworkTransport {
                context,
                encryption: None,
                stream: Arc::new(Mutex::new(server_side)),
                ssl_handler: SslHandler {
                    server_host_name: context.transport_context.get_server_name().clone(),
                    encryption_options: context.encryption_options.clone(),
                },
                stream_recoverer: Box::new(MockStreamRecoverer {}),
                packet_size: context.packet_size as u32,
            },
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
        let mut rng = rand::thread_rng();
        let data_vector: Vec<u8> = (0..MAX_BUFFER_SIZE).map(|_| rng.gen()).collect();

        // Setup the reader to read the data.
        let mut framed_reader = FramedRead::new(server_side, BytesCodec::new());

        // Send the data and read it from the other end of the pipe.
        let result = transport.send(&data_vector[..]).await;
        match result {
            Ok(_) => {}
            Err(e) => panic!("Error sending data: {}", e),
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
        let stream_recoverer = Box::new(MockStreamRecoverer {});
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
        //    (In a real scenario, you'll also set ssl_handler, stream_recoverer, etc.)
        let mut transport = NetworkTransport {
            encryption: None,
            stream: Arc::new(Mutex::new(client_side)),
            ssl_handler,
            stream_recoverer,
            packet_size: context.packet_size as u32,
            context: &context,
        };

        let mut rng = rand::thread_rng();
        let data_size = 128;
        let data_written: Vec<u8> = (0..data_size).map(|_| rng.gen()).collect();
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
