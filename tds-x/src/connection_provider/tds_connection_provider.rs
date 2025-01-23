use crate::connection::client_context::ClientContext;
use crate::connection::tds_connection::TdsConnection;
use crate::connection::transport::network_transport;
use crate::handler::handler_factory::HandlerFactory;
use crate::read_write::reader_writer::NetworkReaderWriterImpl;
use std::io::Error;

pub struct TdsConnectionProvider {}

impl TdsConnectionProvider {
    pub async fn create_connection<'a>(
        &self,
        context: &'a ClientContext,
    ) -> Result<TdsConnection<'a>, Error> {
        let transport_result = network_transport::create_transport(context).await;
        match transport_result {
            Ok(mut result) => {
                let transport_ref = result.as_mut();
                let factory = HandlerFactory { context };

                let mut network_reader_writer = NetworkReaderWriterImpl {
                    transport: transport_ref,
                    packet_size: context.packet_size as u32,
                };

                let session_settings = factory
                    .session_handler()
                    .execute(&mut network_reader_writer)
                    .await;

                Ok(TdsConnection {
                    transport: result,
                    session_settings,
                })
            }
            Err(err) => Err(err),
        }
    }
}
