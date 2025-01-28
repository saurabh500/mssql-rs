use crate::core::Version;
use crate::message::login_options::TdsVersion;
use crate::token::tokens::{Token, TokenType};

#[repr(u8)]
pub enum SqlInterfaceType {
    Default = 0,
    TSql = 1,
}

impl From<u8> for SqlInterfaceType {
    fn from(value: u8) -> Self {
        match value {
            0 => SqlInterfaceType::Default,
            1 => SqlInterfaceType::TSql,
            _ => panic!("Invalid value for SqlInterfaceType"),
        }
    }
}

pub struct LoginAckToken {
    pub interface_type: SqlInterfaceType,
    pub tds_version: TdsVersion,
    pub prog_name: String,
    pub prog_version: Version,
}

impl Token for LoginAckToken {
    fn token_type(&self) -> TokenType {
        TokenType::LoginAck
    }
}
