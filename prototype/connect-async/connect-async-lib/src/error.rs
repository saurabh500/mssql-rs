pub use std::io::ErrorKind as IoErrorKind;
use std::{borrow::Cow, io};
use thiserror::Error;

/// A unified error enum that contains several errors that might occurr during
/// the lifecycle of this driver
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum Error {
    #[error("An error occured during the attempt of performing I/O: {}", message)]
    /// An error occured when performing I/O to the server.
    Io {
        /// A list specifying general categories of I/O error.
        kind: IoErrorKind,
        /// The error description.
        message: String,
    },
    #[error("Protocol error: {}", _0)]
    /// An error happened during the request or response parsing.
    Protocol(Cow<'static, str>),
    #[error("Error forming TLS connection: {}", _0)]
    /// An error in the TLS handshake.
    Tls(String),
}

impl From<async_native_tls::Error> for Error {
    fn from(v: async_native_tls::Error) -> Self {
        Error::Tls(format!("{}", v))
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Self::Io {
            kind: err.kind(),
            message: format!("{}", err),
        }
    }
}
