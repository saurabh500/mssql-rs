// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! TdsTransport trait provides an abstraction over the transport layer for TDS communication.
//! This allows for different implementations (real network, mock for testing/fuzzing, etc.)

use crate::core::TdsResult;
use crate::io::reader_writer::NetworkWriter;
use crate::io::token_stream::TdsTokenStreamReader;
use async_trait::async_trait;

/// TdsTransport abstracts the transport layer for TDS communication.
/// It combines token stream reading capabilities with writer access and reader management.
///
/// This trait is implemented by:
/// - `NetworkTransport` for real network communication
/// - `MockTransport` (in fuzzing mode) for testing without network I/O
#[async_trait]
pub(crate) trait TdsTransport: TdsTokenStreamReader + Send + Sync + std::fmt::Debug {
    /// Get a mutable reference to the network writer.
    /// Used to create packet writers for sending messages to the server.
    fn as_writer(&mut self) -> &mut dyn NetworkWriter;

    /// Reset the internal reader state.
    /// This should clear any buffered data and reset the reader position.
    fn reset_reader(&mut self);

    /// Get the configured packet size for this transport.
    fn packet_size(&self) -> u32;

    /// Close the transport connection.
    /// This should cleanly shut down any underlying network connections.
    async fn close_transport(&mut self) -> TdsResult<()>;
}
