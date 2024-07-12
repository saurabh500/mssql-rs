use tracing::{event, Level};

use super::{into_next,BuilderAction,Connection,Result,Config};
use crate::connection::Transport;

#[derive(Default)]
pub(crate) struct BuilderNoEncryption {
    next: Option<Box<dyn BuilderAction>>,
}

impl BuilderNoEncryption {
    pub(crate) fn new(next: impl BuilderAction + 'static) -> Self {
        Self {
            next: into_next(next),
        }
    }
}

impl BuilderAction for BuilderNoEncryption {
    fn handle(&mut self, connection: &mut Connection, _config: &Config) -> Result<()>  {

        if matches!(connection.transport, Transport::TlsStream(_)) && !connection.encryption_required() {
            let transport = std::mem::replace(&mut connection.transport, Transport::None);
            let stream = transport.into_tcp().unwrap();
            connection.transport = Transport::TcpStream(stream);
            event!(Level::INFO, "Transport udated.");
        } else {
            event!(Level::INFO, "No need to update transport.");
        }
    
        Ok(())
    }

    fn next(&mut self) -> &mut Option<Box<dyn BuilderAction>> {
        &mut self.next
    }
}
