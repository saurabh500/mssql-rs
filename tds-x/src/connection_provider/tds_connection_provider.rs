use crate::connection::client_context::ClientContext;
use crate::connection::tds_connection::TdsConnection;
use crate::connection::transport::network_transport;
use std::io::Error;

pub struct TdsConnectionProvider {}

impl TdsConnectionProvider {
    pub async fn create_connection<'a>(
        &self,
        context: &'a ClientContext,
    ) -> Result<TdsConnection<'a>, Error> {
        let transport_result = network_transport::create_transport(context).await;
        match transport_result {
            Ok(result) => Ok(TdsConnection { transport: result }),
            Err(err) => Err(err),
        }
    }
}
