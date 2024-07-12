//! A prototype of Rust library that shows chain of responsibility (COR) pattern.
//! COR is used in `ConnectionBuilder` with individual steps for creating a connection.
#[macro_use]
mod macros;
mod connection;
mod config;
mod parser;

pub use config::{Config,EncryptionLevel};
pub use parser::Parser;
pub(crate) use connection::Connection;
pub(crate) use connection::builder::ConnectionBuilder;

use thiserror::Error;

/// `TdsError` is an error used in the library. 
/// Any other error is converted to `TdsError`.
/// When the code is integrated to API calls 
/// each error should have a error code as return value from APIs.
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

/// An alias for a result from calls.
pub type Result<T> = std::result::Result<T, TdsError>;
