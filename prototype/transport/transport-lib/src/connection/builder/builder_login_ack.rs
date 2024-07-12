use tracing::{event, Level};

use super::{BuilderAction,Connection,Result,Config,LoginState};
use crate::connection::{Transport,token::decode_token};

#[derive(Default)]
pub(crate) struct BuilderLoginAck {
    next: Option<Box<dyn BuilderAction>>,
}

impl BuilderAction for BuilderLoginAck {
    fn handle(&mut self, connection: &mut Connection, _config: &Config) -> Result<()>  {

        if !matches!(connection.transport, Transport::None) && matches!(connection.login_state, LoginState::Login) {
            let packet = connection.collect_packet()?;
            let (_,mut payload) = packet.into_parts();
            decode_token(&mut payload)?;
            event!(Level::INFO, "Login completed.");
            connection.login_state = LoginState::LoginAck;
        } else {
            event!(Level::INFO, "Login alredy acknowledged.");
        }
    
        Ok(())
    }

    fn next(&mut self) -> &mut Option<Box<dyn BuilderAction>> {
        &mut self.next
    }
}
