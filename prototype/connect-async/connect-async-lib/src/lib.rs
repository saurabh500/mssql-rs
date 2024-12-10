//! A prototype of Rust library that implements TDS connection to a SQL Server.
//! The functionality is limited to SQL authentication.
//! The connection is accomplished in these steps:
//! 1. Create a TCP stream that connects to a SQL Server IP and port.
//! 1. Send a prelogin message to the stream.
//! 1. Get the server prelogin response.
//! 1. Create a TLS stream and initiate a handshake.
//! 1. Handle the TLS handshake by:
//!    - Adding a TDS header when sending the handshake.
//!    - Removing TDS header when receiving the handshake.
//! 1. Send login message to the server over the TLS stream.
//! 1. Switch back to the TCP stream.
//! 1. Receive the server response and decode tokens.
#[macro_use]
mod macros;
pub mod connection;
mod decode;
mod encode;
pub mod error;
mod header;
mod login;
mod packet;
mod pre_login;
mod protocol;
mod token;

pub(crate) use error::Error;
pub type Result<T> = std::result::Result<T, Error>;
const HEADER_BYTES: usize = 8;

struct PacketCodec;
