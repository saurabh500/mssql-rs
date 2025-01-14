use crate::connection::client_context::ClientContext;
use crate::connection::transport::network_transport::Stream;
use async_trait::async_trait;
use std::io::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_native_tls::native_tls::TlsConnector;

#[async_trait(?Send)]
pub trait SslHandler {
    async fn enable_ssl_async(
        &self,
        base_stream: Box<dyn Stream>,
    ) -> Result<(Box<dyn AsyncRead>, Box<dyn AsyncWrite>), Error>;
    fn shutdown_ssl(&self);
}

pub struct Tds8SslHandler<'a> {
    pub settings: &'a ClientContext,
}

#[async_trait(?Send)]
impl<'a> SslHandler for Tds8SslHandler<'a> {
    async fn enable_ssl_async(
        &self,
        base_stream: Box<dyn Stream>,
    ) -> Result<(Box<dyn AsyncRead>, Box<dyn AsyncWrite>), Error> {
        // Build the native TlsConnector directly because tokio-native-tls's version
        // is missing some functionality.
        let connector = TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();

        let encrypted_stream = tokio_native_tls::TlsConnector::from(connector)
            .connect(self.settings.server_name.as_str(), base_stream)
            .await;

        match encrypted_stream {
            Ok(stream) => {
                let (read_half, write_half) = tokio::io::split(stream);
                Ok((Box::new(read_half), Box::new(write_half)))
            }
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
        }
    }

    fn shutdown_ssl(&self) {
        panic!("Cannot disable TLS for TDS version 8.0")
    }
}
