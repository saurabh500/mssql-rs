use crate::connection::client_context::ClientContext;

use super::ssl_handler::SslHandler;

pub trait NetworkTransport {
    fn connect(&self);
    fn send(&self, data: &[u8], start: i32, end: i32);
    fn receive(&self, data: &[u8]) -> i64;

    fn ssl_handler(&self) -> Box<dyn SslHandler>;
}

pub fn create_transport(context: &ClientContext) -> Box<dyn NetworkTransport + '_> {
    Box::new(TcpNetworkTransport { context, socket: 0 })
}

pub struct TcpNetworkTransport<'a> {
    context: &'a ClientContext,
    socket: i32,
}

impl NetworkTransport for TcpNetworkTransport<'_> {
    fn connect(&self) {}
    fn send(&self, _data: &[u8], _start: i32, _end: i32) {}
    fn receive(&self, _data: &[u8]) -> i64 {
        0
    }

    fn ssl_handler(&self) -> Box<dyn SslHandler> {
        struct DummySsl {}

        impl SslHandler for DummySsl {
            fn disable(&self) {}
            fn enable(&self) {}
        }

        Box::new(DummySsl {})
    }
}
