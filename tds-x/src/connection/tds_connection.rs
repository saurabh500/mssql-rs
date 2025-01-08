use super::transport::network_transport::NetworkTransport;

pub struct TdsConnection<'a> {
    pub transport: Box<NetworkTransport<'a>>,
}
