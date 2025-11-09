// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::token::tokens::{Token, TokenType};

pub enum FedAuthInfoId {
    SPN,
    STSUrl,
    Unknown(u8),
}

impl FedAuthInfoId {
    pub fn as_u8(&self) -> u8 {
        match self {
            FedAuthInfoId::SPN => 0x02,
            FedAuthInfoId::STSUrl => 0x01,
            FedAuthInfoId::Unknown(v) => *v,
        }
    }
}

impl From<u8> for FedAuthInfoId {
    fn from(v: u8) -> Self {
        match v {
            0x02 => FedAuthInfoId::SPN,
            0x01 => FedAuthInfoId::STSUrl,
            _ => {
                tracing::warn!("Unknown FedAuthInfoId: 0x{:02X}", v);
                FedAuthInfoId::Unknown(v)
            }
        }
    }
}

#[derive(Debug)]
pub struct FedAuthInfoToken {
    pub spn: String,
    pub sts_url: String,
}

impl Token for FedAuthInfoToken {
    fn token_type(&self) -> TokenType {
        TokenType::FedAuthInfo
    }
}

#[derive(Debug)]
pub struct SspiToken {
    pub data: Vec<u8>,
}

impl Token for SspiToken {
    fn token_type(&self) -> TokenType {
        TokenType::SSPI
    }
}
