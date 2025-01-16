use crate::connection::client_context::ClientContext;
use crate::connection::transport::ssl_handler::{SslHandler, Tds8SslHandler};
use crate::core::EncryptionSetting;
use crate::message::login_options::TdsVersion;
use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;
use std::io::Error;
use tokio::io::{split, AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_util::codec::{BytesCodec, FramedWrite};

pub async fn create_transport(context: &ClientContext) -> Result<Box<NetworkTransport>, Error> {
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
                        reader: encrypted_reader,
                        writer: encrypted_writer,
                        ssl_handler: Box::new(ssl_handler),
                        stream_recoverer: Box::new(stream_recoverer),
                    });
                    Ok(transport)
                }
                Err(err) => Err(err),
            }
        }
        Err(err) => Err(err),
    }
}

#[async_trait(?Send)]
pub trait TransportSslHandler {
    async fn enable_ssl(&mut self) -> Result<(), Error>;
    async fn disable_ssl(&mut self) -> Result<(), Error>;
}

pub trait Stream: AsyncRead + AsyncWrite + Unpin {}

impl<T> Stream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

trait StreamRecoverer {
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

pub struct NetworkTransport<'a> {
    context: &'a ClientContext,
    reader: Box<dyn AsyncRead + Unpin + 'a>,
    writer: Box<dyn AsyncWrite + Unpin + 'a>,
    ssl_handler: Box<dyn SslHandler + 'a>,
    stream_recoverer: Box<dyn StreamRecoverer + 'a>,
}

impl NetworkTransport<'_> {
    async fn send(&mut self, data: &[u8]) -> Result<(), Error> {
        let mut framed = FramedWrite::new(&mut self.writer, BytesCodec::new());
        framed.send(Bytes::copy_from_slice(data)).await?;
        Ok(())
    }

    async fn receive(&self, _data: &[u8]) -> i64 {
        0
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

#[async_trait(?Send)]
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
mod tests {
    use super::*; // Brings in NetworkTransport, SslHandler, StreamRecoverer, etc.
    use crate::connection::client_context::ClientContext;
    use crate::connection::transport::network_transport::Stream; // Your custom trait
    use crate::connection::transport::ssl_handler::SslHandler;
    use async_trait::async_trait;
    use futures::StreamExt;
    use rand::Rng;
    use tokio::io::{duplex, split, AsyncRead, AsyncWrite};
    use tokio_util::codec::{BytesCodec, FramedRead};

    /// A mock SslHandler that simply returns the same stream, no real TLS.
    struct MockSslHandler;

    #[async_trait(?Send)]
    impl SslHandler for MockSslHandler {
        async fn enable_ssl_async(
            &self,
            base_stream: Box<dyn Stream>,
        ) -> Result<(Box<dyn AsyncRead + Unpin>, Box<dyn AsyncWrite + Unpin>), std::io::Error>
        {
            let (r, w) = tokio::io::split(base_stream);
            Ok((Box::new(r), Box::new(w)))
        }

        fn shutdown_ssl(&self) {
            // No-op
        }
    }

    /// A mock StreamRecoverer that always returns the stored DuplexStream.
    struct MockStreamRecoverer {}

    impl StreamRecoverer for MockStreamRecoverer {
        fn recover_base_stream(&self) -> Box<dyn Stream> {
            // Take the DuplexStream out of the option, consuming it.
            let (dummy_client, _) = duplex(1);
            let stream = dummy_client;
            Box::new(stream)
        }
    }

    #[tokio::test]
    async fn test_network_transport_send() {
        // The choice of 8192 is large enough for sending data. This stream should have a buffer large enough for send.
        // The test would keep the payload lower than this size to make sure that the duplex stream can handle it.
        let max_buffer_size = 8192;
        let (client_side, server_side) = tokio::io::duplex(max_buffer_size);

        let (reader, writer) = split(client_side);

        let ssl_handler = Box::new(MockSslHandler);
        let stream_recoverer = Box::new(MockStreamRecoverer {});

        let context = ClientContext::default();

        let mut transport = NetworkTransport {
            context: &context,
            reader: Box::new(reader),
            writer: Box::new(writer),
            ssl_handler,
            stream_recoverer,
        };

        // Fill data_to_send with random values
        let mut rng = rand::thread_rng();
        let data_vector: Vec<u8> = (0..max_buffer_size).map(|_| rng.gen()).collect();

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
}
