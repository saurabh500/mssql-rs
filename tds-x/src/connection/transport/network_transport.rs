use crate::connection::client_context::ClientContext;
use std::io::Error;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use super::ssl_handler::SslHandler;

pub async fn create_transport(context: &ClientContext) -> Result<Box<NetworkTransport>, Error> {
    let connect_result = TcpStream::connect((context.server_name.as_str(), context.port)).await;
    match connect_result {
        Ok(stream) => {
            let (reader, writer) = tokio::io::split(stream);
            Ok(Box::new(NetworkTransport {
                context,
                writer: Arc::new(writer),
                reader: Arc::new(reader),
            }))
        }
        Err(err) => Err(err),
    }
}

pub struct NetworkTransport<'a> {
    context: &'a ClientContext,
    writer: Arc<dyn AsyncWrite>,
    reader: Arc<dyn AsyncRead>,
}

impl NetworkTransport<'_> {
    async fn send(&self, _data: &[u8], _start: i32, _end: i32) {}
    async fn receive(&self, _data: &[u8]) -> i64 {
        0
    }
    fn enable_buffered_reader(&self) {}

    fn ssl_handler(&self) -> Box<dyn SslHandler> {
        struct DummySsl {}

        impl SslHandler for DummySsl {
            fn enable(&self) {}
            fn disable(&self) {}
        }

        Box::new(DummySsl {})
    }
}
