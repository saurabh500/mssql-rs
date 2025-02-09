use crate::connection::client_context::ClientContext;
use crate::connection::transport::ssl_handler::{SslHandler, Tds8SslHandler};
use crate::core::EncryptionSetting;
use crate::handler::handler_factory::SessionSettings;
use crate::message::login_options::TdsVersion;
use crate::message::messages::PacketType;
use crate::read_write::packet_reader::PacketReader;
use crate::read_write::packet_writer::PacketWriter;
use crate::read_write::reader_writer::{NetworkReader, NetworkReaderWriter, NetworkWriter};
use async_trait::async_trait;
use std::io::Error;
use tokio::io::{split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

pub(crate) async fn create_transport(
    context: &ClientContext,
) -> Result<Box<NetworkTransport>, Error> {
    let connect_result = TcpStream::connect((context.server_name.as_str(), context.port)).await;
    match connect_result {
        Ok(stream) => {
            // Enable TLS over TCP immediately in TDS 8.0
            assert!(matches!(context.tds_version(), TdsVersion::V8_0));
            let ssl_handler = Tds8SslHandler { settings: context };

            // Convert the Tokio stream to a std::TcpStream to make it easier to clone.
            // Do this outside the callback instead of making the Tokio Stream part of the closure
            // because into_std() is a destructive operation.
            let std_stream = stream.into_std()?;

            // Define a cloning callback since this is the only time we know we're working with
            // TcpStreams.
            let stream_recoverer = TcpStreamRecoverer {
                stream: Box::new(std_stream),
            };

            let encrypted_stream_result = ssl_handler
                .enable_ssl_async(stream_recoverer.recover_base_stream())
                .await;

            match encrypted_stream_result {
                Ok((encrypted_reader, encrypted_writer)) => {
                    let transport = Box::new(NetworkTransport {
                        context,
                        encryption: context.encryption,
                        reader: encrypted_reader,
                        writer: encrypted_writer,
                        ssl_handler: Box::new(ssl_handler),
                        stream_recoverer: Box::new(stream_recoverer),
                        packet_size: context.packet_size as u32,
                    });
                    Ok(transport)
                }
                Err(err) => Err(err),
            }
        }
        Err(err) => Err(err),
    }
}

#[async_trait]
pub trait TransportSslHandler {
    async fn enable_ssl(&mut self) -> Result<(), Error>;
    async fn disable_ssl(&mut self) -> Result<(), Error>;
}

pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> Stream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

trait StreamRecoverer: Send {
    fn recover_base_stream(&self) -> Box<dyn Stream>;
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
}

pub(crate) struct NetworkTransport<'a> {
    context: &'a ClientContext,
    encryption: EncryptionSetting,
    packet_size: u32,
    reader: Box<dyn AsyncRead + Unpin + Send + 'a>,
    writer: Box<dyn AsyncWrite + Unpin + Send + 'a>,
    ssl_handler: Box<dyn SslHandler + 'a>,
    stream_recoverer: Box<dyn StreamRecoverer + 'a>,
}

impl NetworkReaderWriter for NetworkTransport<'_> {
    fn notify_encryption_setting_change(&mut self, setting: EncryptionSetting) {
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
        self.writer.write_all(data).await?;
        Ok(())
    }

    fn packet_size(&self) -> u32 {
        self.packet_size
    }

    fn get_packet_writer(&mut self, packet_type: PacketType) -> PacketWriter<'_> {
        packet_type.create_packet_writer(self)
    }
}

impl NetworkTransport<'_> {
    pub(crate) fn get_packet_writer(&mut self, packet_type: PacketType) -> PacketWriter<'_> {
        packet_type.create_packet_writer(self)
    }

    pub(crate) async fn send(&mut self, data: &[u8]) -> Result<(), Error> {
        self.writer.write_all(data).await?;
        Ok(())
    }

    pub(crate) fn notify_encryption_negotiation(&mut self, encryption: EncryptionSetting) {
        self.encryption = encryption;
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
        let bytes_read = self.reader.read(buffer).await?;
        Ok(bytes_read)
    }

    async fn enable_ssl_internal(&mut self) -> Result<(), Error> {
        match self
            .ssl_handler
            .enable_ssl_async(self.stream_recoverer.recover_base_stream())
            .await
        {
            Ok((encrypted_reader, encrypted_writer)) => {
                self.reader = encrypted_reader;
                self.writer = encrypted_writer;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn disable_ssl_internal(&mut self) {
        // Notify the SSL handler that SSL is being disabled.
        self.ssl_handler.shutdown_ssl();

        // Update the reader and writer. Note - buffering should get re-enabled on the reader
        // if it was applied previously here.
        let base_stream = self.stream_recoverer.recover_base_stream();
        let (base_reader, base_writer) = split(base_stream);
        self.reader = Box::new(base_reader);
        self.writer = Box::new(base_writer)
    }
}

#[async_trait]
impl TransportSslHandler for NetworkTransport<'_> {
    async fn enable_ssl(&mut self) -> Result<(), Error> {
        self.enable_ssl_internal().await
    }

    async fn disable_ssl(&mut self) -> Result<(), Error> {
        let encryption_type_check = match self.context.encryption {
            EncryptionSetting::NotSupported => Ok(()),
            EncryptionSetting::Optional => Ok(()),
            EncryptionSetting::Required => Ok(()),
            EncryptionSetting::Strict => {
                // TODO: Evaluate this error.
                Err(Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Under strict mode the client must communicate over TLS",
                ))
            }
            EncryptionSetting::LoginOnly => Ok(()),
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
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::SinkExt;
    use futures::StreamExt;
    use rand::Rng;
    use tokio::io::{duplex, split, AsyncRead, AsyncWrite, DuplexStream};
    use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

    // The choice of 8192 is large enough for sending data. This stream should have a buffer large enough for send.
    // The test would keep the payload lower than this size to make sure that the duplex stream can handle it.
    pub(crate) const MAX_BUFFER_SIZE: usize = 8192;

    /// A mock SslHandler that simply returns the same stream, no real TLS.
    pub(crate) struct MockSslHandler;

    #[async_trait]
    impl SslHandler for MockSslHandler {
        async fn enable_ssl_async(
            &self,
            base_stream: Box<dyn Stream>,
        ) -> Result<
            (
                Box<dyn AsyncRead + Send + Unpin>,
                Box<dyn AsyncWrite + Send + Unpin>,
            ),
            std::io::Error,
        > {
            let (r, w) = tokio::io::split(base_stream);
            Ok((Box::new(r), Box::new(w)))
        }

        fn shutdown_ssl(&self) {
            // No-op
        }
    }

    /// A mock StreamRecoverer that always returns the stored DuplexStream.
    pub(crate) struct MockStreamRecoverer {}

    impl StreamRecoverer for MockStreamRecoverer {
        fn recover_base_stream(&self) -> Box<dyn Stream> {
            // Take the DuplexStream out of the option, consuming it.
            let (dummy_client, _) = duplex(1);
            let stream = dummy_client;
            Box::new(stream)
        }
    }

    pub(crate) fn create_readable_network_transport(
        context: &ClientContext,
    ) -> (NetworkTransport, DuplexStream) {
        let (client_side, server_side) = duplex(MAX_BUFFER_SIZE);

        let (reader, writer) = split(client_side);

        let ssl_handler = Box::new(MockSslHandler);
        let stream_recoverer = Box::new(MockStreamRecoverer {});

        (
            NetworkTransport {
                context,
                encryption: context.encryption,
                reader: Box::new(reader),
                writer: Box::new(writer),
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

        let (reader, writer) = split(client_side);
        let (server_reader, server_writer) = split(server_side);

        let ssl_handler = Box::new(MockSslHandler);
        let stream_recoverer = Box::new(MockStreamRecoverer {});

        (
            NetworkTransport {
                context,
                encryption: context.encryption,
                reader: Box::new(reader),
                writer: Box::new(writer),
                ssl_handler,
                stream_recoverer,
                packet_size: context.packet_size as u32,
            },
            NetworkTransport {
                context,
                encryption: context.encryption,
                reader: Box::new(server_reader),
                writer: Box::new(server_writer),
                ssl_handler: Box::new(MockSslHandler),
                stream_recoverer: Box::new(MockStreamRecoverer {}),
                packet_size: context.packet_size as u32,
            },
        )
    }

    #[tokio::test]
    async fn test_network_transport_send() {
        let context = ClientContext::default();
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

        // 2) Split the client side into a reader and writer for the transport
        let (reader, client_writer) = tokio::io::split(client_side);

        // Mocks and defaults.
        let ssl_handler = Box::new(MockSslHandler);
        let stream_recoverer = Box::new(MockStreamRecoverer {});
        let context = ClientContext::default();

        // Optionally, shut down the writer so the reader sees EOF if all data is read
        // client_writer.shutdown().await?;

        // 4) Build our transport
        //    (In a real scenario, you'll also set ssl_handler, stream_recoverer, etc.)
        let mut transport = NetworkTransport {
            encryption: context.encryption,
            reader: Box::new(reader),
            writer: Box::new(client_writer),
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
