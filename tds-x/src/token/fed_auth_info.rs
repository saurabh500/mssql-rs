use crate::token::tokens::{Token, TokenType};

#[repr(u8)]
pub enum FedAuthInfoId {
    SPN = 0x02,
    STSUrl = 0x01,
}

impl From<u8> for FedAuthInfoId {
    fn from(v: u8) -> Self {
        match v {
            0x02 => FedAuthInfoId::SPN,
            0x01 => FedAuthInfoId::STSUrl,
            _ => panic!("Unknown FedAuthInfoId: {}", v),
        }
    }
}

pub struct FedAuthInfoToken {
    pub spn: String,
    pub sts_url: String,
}

impl Token for FedAuthInfoToken {
    fn token_type(&self) -> TokenType {
        TokenType::FedAuthInfo
    }
}
