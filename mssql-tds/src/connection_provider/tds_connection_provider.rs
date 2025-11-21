// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::time::Duration;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::connection::client_context::{ClientContext, TransportContext};
use crate::connection::tds_client::TdsClient;
use crate::connection::transport::network_transport;
use crate::connection::transport::tds_transport::TdsTransport;
use crate::core::{CancelHandle, TdsResult};
use crate::error::Error::{OperationCancelledError, TimeoutError};
use crate::error::{Error, TimeoutErrorType};
use crate::handler::handler_factory::HandlerFactory;
use crate::io::token_stream::GenericTokenParserRegistry;

#[cfg(fuzzing)]
use crate::io::token_stream::TdsTokenStreamReader;

use std::sync::LazyLock;

pub(crate) static PARSER_REGISTRY: LazyLock<GenericTokenParserRegistry> =
    LazyLock::new(GenericTokenParserRegistry::default);

pub struct TdsConnectionProvider;

impl Default for TdsConnectionProvider {
    fn default() -> Self {
        Self
    }
}

impl TdsConnectionProvider {
    /// Create a new TdsConnectionProvider
    pub fn new() -> Self {
        Self
    }

    /// Create a client with a custom transport (used for fuzzing)
    #[cfg(fuzzing)]
    pub async fn create_client_with_transport<T>(
        context: ClientContext,
        transport: T,
    ) -> TdsResult<TdsClient>
    where
        T: TdsTransport
            + crate::io::reader_writer::NetworkReaderWriter
            + TdsTokenStreamReader
            + crate::io::packet_reader::TdsPacketReader
            + 'static,
    {
        let (transport, negotiated_settings, execution_context) =
            Self::connect_with_transport(&context, &context.transport_context, transport).await?;
        Ok(TdsClient::new(
            transport,
            negotiated_settings,
            execution_context,
        ))
    }

    pub async fn create_client(
        &self,
        context: ClientContext,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<TdsClient> {
        CancelHandle::run_until_cancelled(cancel_handle, async move {
            let timeout_duration = match context.connect_timeout {
                1.. => Some(Duration::from_secs(context.connect_timeout.into())),
                _ => None,
            };

            let cancellation_token = cancel_handle.map(|handle| handle.cancel_token.child_token());

            match timeout_duration.as_ref() {
                Some(timeout_duration) => {
                    match timeout(
                        *timeout_duration,
                        Self::create_connection_internal(&context, cancellation_token),
                    )
                    .await
                    {
                        Ok(result) => {
                            let (transport, negotiated_settings, execution_context) = result?;
                            Ok(TdsClient::new(
                                transport,
                                negotiated_settings,
                                execution_context,
                            ))
                        }
                        Err(_) => Err(TimeoutError(TimeoutErrorType::String(
                            "Timeout while connecting".to_string(),
                        ))),
                    }
                }
                None => {
                    let (transport, negotiated_settings, execution_context) =
                        Self::create_connection_internal(&context, cancellation_token).await?;
                    Ok(TdsClient::new(
                        transport,
                        negotiated_settings,
                        execution_context,
                    ))
                }
            }
        })
        .await
    }

    async fn create_connection_internal(
        context: &ClientContext,
        cancellation_token: Option<CancellationToken>,
    ) -> TdsResult<(
        Box<dyn TdsTransport>,
        crate::handler::handler_factory::NegotiatedSettings,
        crate::connection::execution_context::ExecutionContext,
    )> {
        let mut redirect_count = 0;
        let max_redirects = 10;
        let mut connection_result =
            Self::connect_with_transport_context(context, &context.transport_context).await;

        // Loop until we either get a successful connection or we hit the max redirects
        // or an error that is not a redirection
        loop {
            if cancellation_token
                .as_ref()
                .map_or_else(|| false, |token| token.is_cancelled())
            {
                return Err(OperationCancelledError(
                    "Login has been cancelled.".to_string(),
                ));
            };

            match connection_result {
                Ok(connection) => {
                    return Ok(connection);
                }

                Err(err) => match err {
                    // If we get a redirection error, we need to create a new connection
                    // with the new transport context. At this point, it is OK to discard the
                    // previous connection, since it is not useful.
                    Error::Redirection { host, port } => {
                        info!("Redirection to: {:?}, {:?}", host, port);
                        redirect_count += 1;
                        if redirect_count > max_redirects {
                            return Err(Error::ProtocolError(
                                "Received more redirection tokens, than were expected. "
                                    .to_string(),
                            ));
                        }

                        let tcp_transport_context = TransportContext::Tcp { host, port };
                        connection_result =
                            Self::connect_with_transport_context(context, &tcp_transport_context)
                                .await;
                    }
                    _ => return Err(err),
                },
            }
        }
    }

    /// Creates a new connection from the given transport context.
    /// This method will create a new transport and execute the session handler.
    /// If the session handler returns a redirection token, this method will return an error.
    /// If the session handler returns a successful connection, this method will return the connection.
    /// If the session handler returns an error, this method will return the error.
    async fn connect_with_transport_context(
        context: &ClientContext,
        transport_context: &TransportContext,
    ) -> TdsResult<(
        Box<dyn TdsTransport>,
        crate::handler::handler_factory::NegotiatedSettings,
        crate::connection::execution_context::ExecutionContext,
    )> {
        // Create network transport directly
        let mut transport = network_transport::create_transport(
            context.ipaddress_preference,
            context.tds_version(),
            transport_context,
            context.encryption_options.clone(),
        )
        .await?;

        let factory = HandlerFactory {
            context: context.clone(),
        };
        let session_result = factory
            .session_handler(transport_context)
            .execute(&mut *transport)
            .await;

        match session_result {
            Ok(negotiated_settings) => {
                // Create execution context for the new connection
                let execution_context =
                    crate::connection::execution_context::ExecutionContext::new();

                Ok((
                    transport as Box<dyn TdsTransport>,
                    negotiated_settings,
                    execution_context,
                ))
            }
            Err(err) => {
                let _ = transport.close_transport().await;
                Err(err)
            }
        }
    }

    /// Internal generic method that works with a concrete transport type.
    /// This is separated out to allow working with specific transport implementations
    /// (NetworkTransport, MockTransport, etc.) without boxing overhead.
    /// Only exposed for fuzzing.
    #[cfg(fuzzing)]
    async fn connect_with_transport<T>(
        context: &ClientContext,
        transport_context: &TransportContext,
        mut transport: T,
    ) -> TdsResult<(
        Box<dyn TdsTransport>,
        crate::handler::handler_factory::NegotiatedSettings,
        crate::connection::execution_context::ExecutionContext,
    )>
    where
        T: TdsTransport
            + crate::io::reader_writer::NetworkReaderWriter
            + TdsTokenStreamReader
            + crate::io::packet_reader::TdsPacketReader
            + 'static,
    {
        let factory = HandlerFactory {
            context: context.clone(),
        };
        let session_result = factory
            .session_handler(transport_context)
            .execute(&mut transport)
            .await;

        match session_result {
            Ok(negotiated_settings) => {
                // Create execution context for the new connection
                let execution_context =
                    crate::connection::execution_context::ExecutionContext::new();

                Ok((
                    Box::new(transport) as Box<dyn TdsTransport>,
                    negotiated_settings,
                    execution_context,
                ))
            }
            Err(err) => {
                let _ = transport.close_transport().await;
                Err(err)
            }
        }
    }
}
