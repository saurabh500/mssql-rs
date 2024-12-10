use crate::connection::client_context::ClientContext;
use crate::connection::tds_connection::TdsConnection;
use crate::connection::transport::network_transport;

pub struct TdsConnectionProvider {}

impl TdsConnectionProvider {
    pub fn create_connection<'a>(&self, context: &'a ClientContext) -> TdsConnection<'a> {
        TdsConnection {
            transport: network_transport::create_transport(context),
        }
    }
}
