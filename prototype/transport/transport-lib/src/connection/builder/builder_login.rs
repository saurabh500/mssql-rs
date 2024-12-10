use super::LoginMessage;
use tracing::{event, Level};

use super::{into_next, BuilderAction, Config, Connection, LoginState, Result};
use crate::connection::{PacketType, Transport};

#[derive(Default)]
pub(crate) struct BuilderLogin {
    next: Option<Box<dyn BuilderAction>>,
}

impl BuilderLogin {
    pub(crate) fn new(next: impl BuilderAction + 'static) -> Self {
        Self {
            next: into_next(next),
        }
    }
}

impl BuilderAction for BuilderLogin {
    fn handle(&mut self, connection: &mut Connection, config: &Config) -> Result<()> {
        if let Transport::TlsStream(_) = &connection.transport {
            if !matches!(connection.transport, Transport::None)
                && matches!(connection.login_state, LoginState::PreLogin)
            {
                let mut login_message = LoginMessage::new();

                login_message.user_name(config.get_user());
                login_message.password(config.get_password());

                connection.send(PacketType::TDSv7Login, login_message)?;
                event!(Level::INFO, "Login sent.");
                connection.login_state = LoginState::Login;
            } else {
                event!(Level::INFO, "Login already sent.");
            }
        }

        Ok(())
    }

    fn next(&mut self) -> &mut Option<Box<dyn BuilderAction>> {
        &mut self.next
    }
}
