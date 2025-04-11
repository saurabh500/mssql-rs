use tracing::info;

use crate::connection::client_context::{ClientContext, TransportContext};
use crate::connection::tds_connection::{ExecutionContext, TdsConnection};
use crate::connection::transport::network_transport;
use crate::core::TdsResult;
use crate::error::Error;
use crate::handler::handler_factory::HandlerFactory;

pub struct TdsConnectionProvider {}

impl TdsConnectionProvider {
    pub async fn create_connection<'a>(
        &self,
        context: &'a ClientContext,
    ) -> TdsResult<TdsConnection<'a>> {
        let mut redirect_count = 0;
        let max_redirects = 10;
        let mut connection_result = self
            .connect_with_transport_context(context, &context.transport_context)
            .await;

        // Loop until we either get a successful connection or we hit the max redirects
        // or an error that is not a redirection
        loop {
            match connection_result {
                Ok(connection) => {
                    return Ok(connection);
                }

                Err(err) => match err {
                    // If we get a redirection error, we need to create a new connection
                    // with the new transport context. At this point, it is OK to discard the
                    // previous connection, since it is not useful.
                    Error::Redirection { host, port } => {
                        info!("Redirection to: {:?}, {:?}", host, port);
                        redirect_count += 1;
                        if redirect_count > max_redirects {
                            return Err(Error::ProtocolError(
                                "Received more redirection tokens, than were expected. "
                                    .to_string(),
                            ));
                        }

                        let tcp_transport_context = TransportContext::Tcp { host, port };
                        connection_result = self
                            .connect_with_transport_context(context, &tcp_transport_context)
                            .await;
                    }
                    _ => return Err(err),
                },
            }
        }
    }

    /// Creates a new connection from the given transport context.
    /// This method will create a new transport and execute the session handler.
    /// If the session handler returns a redirection token, this method will return an error.
    /// If the session handler returns a successful connection, this method will return the connection.
    /// If the session handler returns an error, this method will return the error.
    async fn connect_with_transport_context<'a>(
        &self,
        context: &'a ClientContext,
        transport_context: &TransportContext,
    ) -> TdsResult<TdsConnection<'a>> {
        // Create transport
        let transport_result =
            network_transport::create_transport(context, transport_context).await;

        match transport_result {
            Ok(mut transport) => {
                // Create the handler factory and execute the session
                let factory = HandlerFactory { context };
                let session_result = factory
                    .session_handler(transport_context)
                    .execute(transport.as_mut())
                    .await;

                match session_result {
                    Ok(negotiated_settings) => Ok(TdsConnection {
                        transport,
                        negotiated_settings,
                        execution_context: ExecutionContext::new(),
                    }),
                    Err(err) => {
                        transport.close_transport().await?;
                        Err(err)
                    }
                }
            }

            Err(err) => Err(err),
        }
    }
}
