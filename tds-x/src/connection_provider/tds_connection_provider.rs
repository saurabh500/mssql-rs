use crate::connection::client_context::ClientContext;
use crate::connection::tds_connection::TdsConnection;
use crate::connection::transport::network_transport;
use crate::core::TdsResult;
use crate::handler::handler_factory::HandlerFactory;

pub struct TdsConnectionProvider {}

impl TdsConnectionProvider {
    pub async fn create_connection<'a>(
        &self,
        context: &'a ClientContext,
    ) -> TdsResult<TdsConnection<'a>> {
        let transport_result = network_transport::create_transport(context).await;
        match transport_result {
            Ok(mut result) => {
                // let transport_ref = result.as_mut();
                let factory = HandlerFactory { context };

                let negotiated_settings =
                    factory.session_handler().execute(result.as_mut()).await?;

                Ok(TdsConnection {
                    transport: result,
                    negotiated_settings,
                })
            }
            Err(err) => Err(err),
        }
    }
}
