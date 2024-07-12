use super::{into_next,BuilderAction,Connection,Result,Config};
use crate::connection::Transport;
use std::net::TcpStream;
use tracing::{event, Level};

#[derive(Default)]
pub(crate) struct BuilderTcp {
    next: Option<Box<dyn BuilderAction>>,
}

impl BuilderTcp {
    pub(crate) fn new(next: impl BuilderAction + 'static) -> Self {
        Self {
            next: into_next(next),
        }
    }
}

impl BuilderAction for BuilderTcp {
    fn handle(&mut self, connection: &mut Connection, config: &Config) -> Result<()>  {
        if config.is_tcp() && matches!(connection.transport, Transport::None) {
            event!(Level::TRACE, "No trasport creating TCP stream.");
            let tcp_stream = TcpStream::connect(config.get_host())?;
            connection.transport = Transport::TcpStream(tcp_stream);
            event!(Level::TRACE, "Created TCP stream");
        } else {
            event!(Level::INFO, "TCP stream is already created.");
        }

        Ok(())
    }

    fn next(&mut self) -> &mut Option<Box<dyn BuilderAction>> {
        &mut self.next
    }
}