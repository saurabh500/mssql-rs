#[macro_use]
mod macros;
mod callback;
mod pull_buffer;
mod push_buffer;

use bytes::BytesMut;
use std::convert::From;
use thiserror::Error;

pub trait Parse {
    fn parse(&self, buf: BytesMut) -> crate::Result<()>;
}

fn parse_all<T: Parse>(parser: &T) -> crate::Result<()> {
    let db_token: [u8; 0x1B + 2] = [
        0x1B, 0x00, 0x01, 0x06, 0x6D, 0x00, 0x61, 0x00, 0x73, 0x00, 0x74, 0x00, 0x65, 0x00, 0x72,
        0x00, 0x06, 0x6D, 0x00, 0x61, 0x00, 0x73, 0x00, 0x74, 0x00, 0x65, 0x00, 0x72, 0x00,
    ];
    let collation_token: [u8; 0x08 + 2] =
        [0x08, 0x00, 0x07, 0x05, 0x09, 0x04, 0xD0, 0x00, 0x34, 0x00];
    let packet_size_token: [u8; 0x13 + 2] = [
        0x13, 0x00, 0x04, 0x04, 0x34, 0x00, 0x30, 0x00, 0x39, 0x00, 0x36, 0x00, 0x04, 0x34, 0x00,
        0x30, 0x00, 0x39, 0x00, 0x36, 0x00,
    ];

    let mut buf = BytesMut::new();
    buf.extend_from_slice(&db_token);
    parser.parse(buf)?;
    buf = BytesMut::new();
    buf.extend_from_slice(&collation_token);
    parser.parse(buf)?;
    buf = BytesMut::new();
    buf.extend_from_slice(&packet_size_token);
    parser.parse(buf)?;

    Ok(())
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum TdsError {
    #[error("An error occured: {}", _0)]
    Message(String),
}

impl From<std::io::Error> for TdsError {
    fn from(err: std::io::Error) -> Self {
        Self::Message(format!("IO error occured: {}", err))
    }
}

impl From<std::string::FromUtf16Error> for TdsError {
    fn from(err: std::string::FromUtf16Error) -> Self {
        Self::Message(format!("A conversion error occured:{}", err))
    }
}

impl From<std::num::ParseIntError> for TdsError {
    fn from(err: std::num::ParseIntError) -> TdsError {
        Self::Message(format!("A parse error occured:{}", err))
    }
}

pub type Result<T> = std::result::Result<T, TdsError>;
