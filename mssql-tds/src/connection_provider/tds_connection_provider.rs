// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(deprecated)]

use std::time::Duration;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::connection::client_context::{ClientContext, TransportContext};
use crate::connection::tds_client::TdsClient;
use crate::connection::tds_connection::TdsConnection;
use crate::connection::transport::network_transport;
use crate::core::{CancelHandle, TdsResult};
use crate::error::Error::{OperationCancelledError, TimeoutError};
use crate::error::{Error, TimeoutErrorType};
use crate::handler::handler_factory::HandlerFactory;
use crate::read_write::token_stream::GenericTokenParserRegistry;

use std::sync::LazyLock;

pub(crate) static PARSER_REGISTRY: LazyLock<GenericTokenParserRegistry> =
    LazyLock::new(GenericTokenParserRegistry::default);

pub struct TdsConnectionProvider {}

impl TdsConnectionProvider {
    /// Creates a new TdsConnection with streaming API.
    ///
    /// # Deprecation Notice
    ///
    /// **This method is deprecated. Use [`create_client`](Self::create_client) instead.**
    ///
    /// `create_client` provides a cursor-based API with better ergonomics and safety.
    /// See [`TdsConnection`](crate::connection::tds_connection::TdsConnection) for migration guide.
    #[deprecated(
        since = "0.2.0",
        note = "Use create_client() instead for the cursor-based TdsClient API."
    )]
    pub async fn create_connection(
        &self,
        context: ClientContext,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<TdsConnection> {
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
                        self.create_connection_internal(&context, cancellation_token),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(TimeoutError(TimeoutErrorType::String(
                            "Timeout while connecting".to_string(),
                        ))),
                    }
                }
                None => {
                    self.create_connection_internal(&context, cancellation_token)
                        .await
                }
            }
        })
        .await
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
                        self.create_connection_internal(&context, cancellation_token),
                    )
                    .await
                    {
                        Ok(result) => {
                            let result = result?;
                            Ok(TdsClient::new(
                                result.transport,
                                result.negotiated_settings,
                                result.execution_context,
                            ))
                        }
                        Err(_) => Err(TimeoutError(TimeoutErrorType::String(
                            "Timeout while connecting".to_string(),
                        ))),
                    }
                }
                None => {
                    let connection = self
                        .create_connection_internal(&context, cancellation_token)
                        .await?;
                    Ok(TdsClient::new(
                        connection.transport,
                        connection.negotiated_settings,
                        connection.execution_context,
                    ))
                }
            }
        })
        .await
    }

    async fn create_connection_internal(
        &self,
        context: &ClientContext,
        cancellation_token: Option<CancellationToken>,
    ) -> TdsResult<TdsConnection> {
        let mut redirect_count = 0;
        let max_redirects = 10;
        let mut connection_result = self
            .connect_with_transport_context(context, &context.transport_context)
            .await;

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
                        connection_result = self
                            .connect_with_transport_context(context, &tcp_transport_context)
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
        &self,
        context: &ClientContext,
        transport_context: &TransportContext,
    ) -> TdsResult<TdsConnection> {
        // Create transport
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
            .execute(transport.as_mut())
            .await;

        match session_result {
            Ok(negotiated_settings) => Ok(TdsConnection::new(transport, negotiated_settings)),
            Err(err) => {
                transport.close_transport().await?;
                Err(err)
            }
        }
    }
}
