use crate::core::Version;
use crate::message::login_options::TdsVersion;
use crate::token::tokens::{Token, TokenType};

pub enum SqlInterfaceType {
    Default = 0,
    TSql = 1,
}

pub struct LoginAck {
    pub interface_type: SqlInterfaceType,
    pub tds_version: TdsVersion,
    pub prog_name: String,
    pub prog_version: Version,
}

impl Token for LoginAck {
    fn token_type(&self) -> TokenType {
        TokenType::LoginAck
    }
}
