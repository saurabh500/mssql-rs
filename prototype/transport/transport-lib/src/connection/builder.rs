mod builder_login;
mod builder_login_ack;
mod builder_no_encryption;
mod builder_pre_login;
mod builder_tcp;
mod builder_tls;

pub(crate) use super::token::login::LoginMessage;
pub(crate) use super::token::pre_login::PreloginMessage;
use super::{Connection, LoginState, Result};
pub use crate::Config;
use builder_login::BuilderLogin;
use builder_login_ack::BuilderLoginAck;
use builder_no_encryption::BuilderNoEncryption;
use builder_pre_login::BuilderPreLogin;
use builder_tcp::BuilderTcp;
use builder_tls::BuilderTls;

pub struct ConnectionBuilder {}

impl ConnectionBuilder {
    pub fn new() -> ConnectionBuilder {
        ConnectionBuilder {}
    }

    pub fn create(&self, connection: &mut Connection, config: &Config) -> Result<()> {
        let mut validate = BuilderTcp::new(BuilderPreLogin::new(BuilderTls::new(
            BuilderLogin::new(BuilderNoEncryption::new(BuilderLoginAck::default())),
        )));

        validate.execute(connection, config)?;
        Ok(())
    }

    pub fn build(&self, config: &Config) -> Result<Connection> {
        let mut connection = Connection::new();
        self.create(&mut connection, config)?;
        Ok(connection)
    }
}

pub trait BuilderAction {
    fn execute(&mut self, connection: &mut Connection, config: &Config) -> Result<()> {
        self.handle(connection, config)?;

        if let Some(next) = &mut self.next() {
            next.execute(connection, config)?;
        }

        Ok(())
    }

    fn handle(&mut self, connection: &mut Connection, config: &Config) -> Result<()>;
    fn next(&mut self) -> &mut Option<Box<dyn BuilderAction>>;
}

/// Helps to wrap an object into a boxed type.
pub fn into_next(action: impl BuilderAction + Sized + 'static) -> Option<Box<dyn BuilderAction>> {
    Some(Box::new(action))
}
