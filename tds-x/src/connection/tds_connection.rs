use super::transport::network_transport::NetworkTransport;
use crate::handler::handler_factory::SessionSettings;

pub struct TdsConnection<'a> {
    pub transport: Box<NetworkTransport<'a>>,
    pub(crate) session_settings: SessionSettings,
}
