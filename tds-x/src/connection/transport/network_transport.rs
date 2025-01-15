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

impl Stream for TcpStream {}

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
    async fn send(&mut self, data: &[u8], start: i32, end: i32) {
        if start < 0 || end < start || end as usize > data.len() {
            panic!(
                "Invalid range: start={} end={} data_len={}",
                start,
                end,
                data.len()
            );
        }

        // Extract slice of data
        let slice = &data[start as usize..end as usize];

        let mut framed = FramedWrite::new(&mut self.writer, BytesCodec::new());

        // TODO: Handle exceptions better.
        framed
            .send(Bytes::copy_from_slice(slice))
            .await
            .expect("Failed to write data");
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
