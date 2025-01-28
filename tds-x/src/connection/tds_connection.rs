use super::transport::network_transport::NetworkTransport;
use crate::handler::handler_factory::NegotiatedSettings;

pub struct TdsConnection<'a> {
    pub(crate) transport: Box<NetworkTransport<'a>>,
    pub(crate) negotiated_settings: NegotiatedSettings,
}
