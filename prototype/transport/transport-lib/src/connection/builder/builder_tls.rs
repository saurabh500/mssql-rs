use super::{into_next, BuilderAction, Config, Connection, Result};
use crate::connection::transport::TlsTransport;
use crate::connection::Transport;
use crate::TdsError;
use native_tls::TlsConnector;
use native_tls::TlsStream;
use std::io::{Read, Write};
use tracing::{event, Level};

#[derive(Default)]
pub(crate) struct BuilderTls {
    next: Option<Box<dyn BuilderAction>>,
}

impl BuilderTls {
    pub(crate) fn new(next: impl BuilderAction + 'static) -> Self {
        Self {
            next: into_next(next),
        }
    }
}

impl BuilderAction for BuilderTls {
    fn handle(&mut self, connection: &mut Connection, config: &Config) -> Result<()> {
        if config.is_tcp()
            && connection.pending_handshake
            && matches!(connection.transport, Transport::TcpStream(_))
        {
            event!(Level::TRACE, "No TLS handshake.");

            let transport = std::mem::replace(&mut connection.transport, Transport::None);
            if let Transport::TcpStream(stream) = transport {
                let mut tls_stream =
                    create_tls_stream(config.get_host(), TlsTransport::new(stream))?;
                tls_stream.get_mut().handshake_complete();
                connection.transport = Transport::TlsStream(tls_stream);
                connection.pending_handshake = false;
            }
            event!(Level::TRACE, "TLS handshake completed.");
        } else {
            event!(Level::TRACE, "TLS handshake is already done.");
        }

        Ok(())
    }

    fn next(&mut self) -> &mut Option<Box<dyn BuilderAction>> {
        &mut self.next
    }
}

fn create_tls_stream<S: Read + Write>(host: &str, stream: S) -> Result<TlsStream<S>> {
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .use_sni(false)
        .build()
        .unwrap();

    let result = connector.connect(host, stream);
    match result {
        Ok(stream) => Ok(stream),
        Err(_e) => Err(TdsError::Message("Handshake failed".to_string())),
    }
}
