use tracing::{event, Level};

use super::{BuilderAction,Connection,Result,Config,LoginState};
use crate::connection::{token::decode_token, transport::TransportBuffer, Transport};

#[derive(Default)]
pub(crate) struct BuilderLoginAck {
    next: Option<Box<dyn BuilderAction>>,
}

impl BuilderAction for BuilderLoginAck {
    fn handle(&mut self, connection: &mut Connection, _config: &Config) -> Result<()>  {

        if !matches!(connection.transport, Transport::None) && matches!(connection.login_state, LoginState::Login) {
            connection.collect_token_packet()?;
            decode_token(connection)?;
            event!(Level::INFO, "Login completed.");
            connection.login_state = LoginState::LoginAck;
            if connection.is_eof() {
                event!(Level::INFO, "Buffer processed.");
            } else {
                event!(Level::WARN, "Buffer NOT processed.");
            }
        } else {
            event!(Level::INFO, "Login alredy acknowledged.");
        }
    
        Ok(())
    }

    fn next(&mut self) -> &mut Option<Box<dyn BuilderAction>> {
        &mut self.next
    }
}
