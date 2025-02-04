use super::transport::network_transport::NetworkTransport;
use crate::handler::handler_factory::NegotiatedSettings;
use crate::query::result::QueryResult;

pub struct TdsConnection<'a> {
    pub(crate) transport: Box<NetworkTransport<'a>>,
    pub(crate) negotiated_settings: NegotiatedSettings,
}

impl TdsConnection<'_> {
    pub async fn execute(&self, _query_text: String) -> QueryResult {
        todo!()
    }
}
