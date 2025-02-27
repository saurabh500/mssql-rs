use crate::connection::client_context::ClientContext;
use crate::connection::transport::ssl_handler::{SslHandler, Tds7StreamRecoverer};
use crate::core::{EncryptionSetting, NegotiatedEncryptionSetting};
use crate::handler::handler_factory::SessionSettings;
use crate::message::login_options::TdsVersion;
use crate::message::messages::PacketType;
use crate::read_write::packet_reader::PacketReader;
use crate::read_write::packet_writer::PacketWriter;
use crate::read_write::reader_writer::{NetworkReader, NetworkReaderWriter, NetworkWriter};
use async_trait::async_trait;
use std::io::{Error, ErrorKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

pub(crate) async fn create_transport(
    context: &ClientContext,
) -> Result<Box<NetworkTransport>, Error> {
    let stream = TcpStream::connect((context.server_name.as_str(), context.port)).await?;

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
                stream: base_stream,
                ssl_handler: SslHandler { settings: context },
                stream_recoverer: Box::new(stream_recoverer),
                packet_size: context.packet_size as u32,
            }))
        }
        TdsVersion::V8_0 => {
            let stream_recoverer = TcpStreamRecoverer {
                stream: Box::new(std_stream),
            };

            // Enable TLS over TCP immediately in TDS 8.0
            let ssl_handler = SslHandler { settings: context };
            let encrypted_stream = ssl_handler
                .enable_ssl_async(stream_recoverer.recover_base_stream())
                .await?;

            Ok(Box::new(NetworkTransport {
                context,
                encryption: None,
                stream: encrypted_stream,
                ssl_handler,
                stream_recoverer: Box::new(stream_recoverer),
                packet_size: context.packet_size as u32,
            }))
        }
    }
}

#[async_trait]
pub trait TransportSslHandler {
    async fn enable_ssl(&mut self) -> Result<(), Error>;
    async fn disable_ssl(&mut self) -> Result<(), Error>;
}

pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send {
    fn tls_handshake_starting(&mut self);
    fn tls_handshake_completed(&mut self);
}

pub(crate) trait StreamRecoverer: Send {
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
    stream: Box<dyn Stream>,
    ssl_handler: SslHandler<'a>,
    stream_recoverer: Box<dyn StreamRecoverer + 'a>,
}

impl NetworkReaderWriter for NetworkTransport<'_> {
    fn notify_encryption_setting_change(&mut self, setting: NegotiatedEncryptionSetting) {
        self.notify_encryption_negotiation(setting);
    }

    fn notify_session_setting_change(&mut self, setting: &SessionSettings) {
        self.packet_size = setting.packet_size;
    }
}

#[async_trait]
impl NetworkReader for NetworkTransport<'_> {
    async fn receive(&mut self, buffer: &mut [u8]) -> Result<usize, Error> {
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
    async fn send(&mut self, data: &[u8]) -> Result<(), Error> {
        self.stream.write_all(data).await?;
        Ok(())
    }

    fn packet_size(&self) -> u32 {
        self.packet_size
    }

    fn get_packet_writer(&mut self, packet_type: PacketType) -> PacketWriter<'_> {
        packet_type.create_packet_writer(self)
    }

    fn get_encryption_setting(&self) -> NegotiatedEncryptionSetting {
        assert!(self.encryption.is_some());
        self.encryption.unwrap()
    }
}

impl NetworkTransport<'_> {
    pub(crate) fn get_packet_writer(&mut self, packet_type: PacketType) -> PacketWriter<'_> {
        packet_type.create_packet_writer(self)
    }

    pub(crate) async fn send(&mut self, data: &[u8]) -> Result<(), Error> {
        self.stream.write_all(data).await?;
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
    /// Returns the number of bytes actually read, or an [`std::io::Error`] if
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
    /// # async fn demo(mut transport: NetworkTransport<'_>) -> Result<(), Error> {
    ///     let mut buf = [0u8; 1024];
    ///     let bytes_read = transport.receive(&mut buf).await?;
    ///     println!("Read {} bytes", bytes_read);
    ///     Ok(())
    /// # }
    /// ```
    pub(crate) async fn receive(&mut self, buffer: &mut [u8]) -> Result<usize, Error> {
        if buffer.is_empty() {
            panic!("Buffer length must be greater than 0");
        }
        let bytes_read = self.stream.read(buffer).await?;
        if bytes_read == 0 {
            Err(Error::from(ErrorKind::UnexpectedEof))
        } else {
            Ok(bytes_read)
        }
    }

    async fn enable_ssl_internal(&mut self) -> Result<(), Error> {
        self.stream_recoverer.tls_handshake_starting();
        let encrypted_stream = self
            .ssl_handler
            .enable_ssl_async(self.stream_recoverer.recover_base_stream())
            .await?;

        self.stream_recoverer.tls_handshake_completed();
        self.stream = encrypted_stream;
        Ok(())
    }

    async fn disable_ssl_internal(&mut self) {
        let base_stream = self.stream_recoverer.recover_base_stream();
        self.stream = base_stream;
    }
}

#[async_trait]
impl TransportSslHandler for NetworkTransport<'_> {
    async fn enable_ssl(&mut self) -> Result<(), Error> {
        self.enable_ssl_internal().await
    }

    async fn disable_ssl(&mut self) -> Result<(), Error> {
        let encryption_type_check = match self.context.encryption {
            EncryptionSetting::Strict => {
                // TODO: Evaluate this error.
                Err(Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Under strict mode the client must communicate over TLS",
                ))
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

        let ssl_handler = SslHandler { settings: context };
        let stream_recoverer = Box::new(MockStreamRecoverer {});

        (
            NetworkTransport {
                context,
                encryption: None,
                stream: Box::new(client_side),
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

        let ssl_handler = SslHandler { settings: context };
        let stream_recoverer = Box::new(MockStreamRecoverer {});

        (
            NetworkTransport {
                context,
                encryption: None,
                stream: Box::new(client_side),
                ssl_handler,
                stream_recoverer,
                packet_size: context.packet_size as u32,
            },
            NetworkTransport {
                context,
                encryption: None,
                stream: Box::new(server_side),
                ssl_handler: SslHandler { settings: context },
                stream_recoverer: Box::new(MockStreamRecoverer {}),
                packet_size: context.packet_size as u32,
            },
        )
    }

    #[tokio::test]
    async fn test_network_transport_send() {
        let mut context = ClientContext::new();
        context.encryption = EncryptionSetting::Strict;
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
    async fn test_network_transport_receive() -> Result<(), Error> {
        // 1) Create an in-memory duplex stream (client_side, server_side).
        // Data will be written on the server_side and the network transport will read from the client side.
        let (client_side, server_side) = duplex(1024);

        // Mocks and defaults.
        let stream_recoverer = Box::new(MockStreamRecoverer {});
        let mut context = ClientContext::new();
        context.encryption = EncryptionSetting::Strict;
        let ssl_handler = SslHandler { settings: &context };

        // Optionally, shut down the writer so the reader sees EOF if all data is read
        // client_writer.shutdown().await?;

        // Build our transport
        //    (In a real scenario, you'll also set ssl_handler, stream_recoverer, etc.)
        let mut transport = NetworkTransport {
            encryption: None,
            stream: Box::new(client_side),
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
