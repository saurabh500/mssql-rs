// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Named Pipe transport implementation for Windows
//!
//! This module provides Windows-specific functionality for connecting to SQL Server
//! via Named Pipes, including retry logic for busy pipe instances.

use crate::connection::client_context::TransportContext;
use crate::connection::transport::network_transport::{
    NetworkTransport, PRE_NEGOTIATED_PACKET_SIZE, Stream,
};
use crate::connection::transport::ssl_handler::SslHandler;
use crate::core::{EncryptionOptions, EncryptionSetting, TdsResult};
use std::time::Duration;
use tokio::net::windows::named_pipe::NamedPipeClient;
use tracing::{debug, info, warn};

use std::ffi::OsStr;
use std::os::windows::ffi::OsStrExt;
use winapi::shared::winerror::ERROR_PIPE_BUSY;

/// Timeout for Named Pipe connection attempts (matching ODBC's NP_OPEN_TIMEOUT)
pub(crate) const NAMED_PIPE_OPEN_TIMEOUT_MS: u32 = 5000;

/// Opens a named pipe with retry logic to handle ERROR_PIPE_BUSY (231).
///
/// When all instances of a named pipe are busy, Windows returns ERROR_PIPE_BUSY.
/// This function uses WaitNamedPipeW to wait for a pipe instance to become available,
/// then retries the connection. This matches ODBC driver behavior.
///
/// Timeout: NAMED_PIPE_OPEN_TIMEOUT_MS (5000ms by default)
pub(crate) async fn open_named_pipe_with_retry(
    pipe_path: &str,
) -> std::io::Result<NamedPipeClient> {
    use std::time::Instant;
    use tokio::net::windows::named_pipe::ClientOptions;

    info!(pipe_path, "Opening named pipe connection");
    let start_time = Instant::now();
    let timeout_duration = Duration::from_millis(NAMED_PIPE_OPEN_TIMEOUT_MS as u64);

    loop {
        match ClientOptions::new()
            .pipe_mode(tokio::net::windows::named_pipe::PipeMode::Message)
            .open(pipe_path)
        {
            Ok(client) => {
                debug!(pipe_path, elapsed_ms = ?start_time.elapsed().as_millis(), "Named pipe connection established");
                return Ok(client);
            }
            Err(e) => {
                // ERROR_PIPE_BUSY - All pipe instances are busy
                if e.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) {
                    let elapsed = start_time.elapsed();
                    warn!(pipe_path, elapsed_ms = ?elapsed.as_millis(), "Named pipe busy, waiting for available instance");
                    if elapsed >= timeout_duration {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!(
                                "Named pipe connection timed out after {}ms: all pipe instances busy",
                                elapsed.as_millis()
                            ),
                        ));
                    }

                    // Calculate remaining timeout
                    let remaining_ms = timeout_duration
                        .checked_sub(elapsed)
                        .unwrap_or(Duration::from_millis(0))
                        .as_millis() as u32;

                    if remaining_ms == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Named pipe connection timed out: all pipe instances busy",
                        ));
                    }

                    // Wait for pipe to become available (synchronous Windows API call)
                    // Use spawn_blocking to avoid blocking the tokio runtime
                    let pipe_path_owned = pipe_path.to_string();
                    match tokio::task::spawn_blocking(move || {
                        wait_for_named_pipe(&pipe_path_owned, remaining_ms)
                    })
                    .await
                    {
                        Ok(Ok(())) => {
                            // Pipe should be available now, retry CreateFile
                            debug!("Named pipe became available, retrying connection");
                            continue;
                        }
                        Ok(Err(wait_err)) => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                format!(
                                    "Named pipe wait failed after {}ms: {}",
                                    elapsed.as_millis(),
                                    wait_err
                                ),
                            ));
                        }
                        Err(join_err) => {
                            return Err(std::io::Error::other(format!(
                                "Failed to wait for named pipe: {join_err}"
                            )));
                        }
                    }
                } else {
                    // For any other error, fail immediately
                    return Err(e);
                }
            }
        }
    }
}

/// Synchronous helper function that calls WaitNamedPipeW to wait for a pipe instance.
/// This function blocks until a pipe instance is available or the timeout expires.
///
/// # Arguments
/// * `pipe_path` - The full path to the named pipe (e.g., r"\\.\pipe\SQLLocal\MSSQLSERVER")
/// * `timeout_ms` - Timeout in milliseconds
fn wait_for_named_pipe(pipe_path: &str, timeout_ms: u32) -> std::io::Result<()> {
    use winapi::um::namedpipeapi::WaitNamedPipeW;

    debug!(pipe_path, timeout_ms, "Calling WaitNamedPipeW");

    // Convert pipe path to wide string (UTF-16)
    let wide_path: Vec<u16> = OsStr::new(pipe_path)
        .encode_wide()
        .chain(std::iter::once(0)) // Null terminator
        .collect();

    // Call WaitNamedPipeW (synchronous Windows API)
    // Returns:
    //   TRUE (non-zero) if a pipe instance is available
    //   FALSE (0) if timeout expires or error occurs
    let result = unsafe { WaitNamedPipeW(wide_path.as_ptr(), timeout_ms) };

    if result == 0 {
        // WaitNamedPipeW failed or timed out
        return Err(std::io::Error::last_os_error());
    }

    Ok(())
}

/// Creates a NetworkTransport for Named Pipe connections.
///
/// Named Pipes support TLS encryption, and this function sets up the transport
/// with appropriate SSL handling. Uses the transport context to extract the
/// server name for TLS certificate validation.
pub(crate) async fn create_named_pipe_transport(
    pipe_client: NamedPipeClient,
    transport_context: &TransportContext,
    encryption_options: EncryptionOptions,
    encryption_mode: EncryptionSetting,
) -> TdsResult<Box<NetworkTransport>> {
    // Named Pipes support TLS encryption
    let base_stream: Box<dyn Stream> = Box::new(pipe_client);

    // Extract server name from the transport context
    // This handles both local (\\.\\...) and remote (\\\\server\\...) pipe paths
    let server_host_name = transport_context.get_server_name();
    info!(server_host_name, ?encryption_mode, "Creating named pipe transport");

    Ok(Box::new(NetworkTransport::new(
        base_stream,
        SslHandler {
            server_host_name,
            encryption_options,
        },
        PRE_NEGOTIATED_PACKET_SIZE,
        encryption_mode,
    )))
}

/// Implementation of Stream trait for NamedPipeClient
impl Stream for NamedPipeClient {
    fn tls_handshake_starting(&mut self) {
        // No-op for named pipe streams
    }

    fn tls_handshake_completed(&mut self) {
        // No-op for named pipe streams
    }
}
