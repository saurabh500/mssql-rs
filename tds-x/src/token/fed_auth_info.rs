use crate::token::tokens::{Token, TokenType};

pub enum FedAuthInfoId {
    SPN = 0x02,
    STSUrl = 0x01,
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
