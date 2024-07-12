use super::PreloginMessage;
use tracing::{event, Level};
use super::{into_next,BuilderAction,Connection,Result,Config,LoginState};
use crate::{connection::{Decode,PacketType,Transport}, TdsError};

#[derive(Default)]
pub(crate) struct BuilderPreLogin {
    next: Option<Box<dyn BuilderAction>>,
}

impl BuilderPreLogin {
    pub(crate) fn new(next: impl BuilderAction + 'static) -> Self {
        Self {
            next: into_next(next),
        }
    }
}

impl BuilderAction for BuilderPreLogin {
    fn handle(&mut self, connection: &mut Connection, _config: &Config) -> Result<()>  {
        if !matches!(connection.transport, Transport::None)
            && (connection.server_encryption.is_none()
            || matches!(connection.login_state, LoginState::None)) {
            let msg = PreloginMessage::new();
            connection.send(PacketType::PreLogin, msg)?;

            let packet = connection.collect_packet()?;
            let (header,mut payload) = packet.into_parts();
            if header.get_type() != PacketType::TabularResult {
                return Err(TdsError::Message(format!("Invalid packet type {:?}, expected TabularResult.", header.get_type())));
            }
            let response = PreloginMessage::decode(&mut payload)?;
            
            event!(Level::INFO, "Prelogin: {:?}", response);
            let bytes = response.version.to_be_bytes();
            event!(Level::INFO, "Server version: {}.{}.{}", bytes[0], bytes[1], ((bytes[2] as u16) << 8) + (bytes[3] as u16));
    
            connection.fed_auth_required = response.fed_auth_required;
            connection.server_encryption = Some(response.encryption);
            connection.login_state = LoginState::PreLogin;
        } else {
            event!(Level::INFO, "Prelogin is already done");
        }
        
        Ok(())
    }

    fn next(&mut self) -> &mut Option<Box<dyn BuilderAction>> {
        &mut self.next
    }
}
