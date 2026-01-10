// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::time::Duration;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::connection::client_context::{ClientContext, TransportContext};
use crate::connection::tds_client::TdsClient;
use crate::connection::transport::network_transport;
use crate::connection::transport::tds_transport::TdsTransport;
use crate::core::{CancelHandle, TdsResult};
use crate::error::Error::{OperationCancelledError, TimeoutError};
use crate::error::{Error, TimeoutErrorType};
use crate::handler::handler_factory::HandlerFactory;
use crate::io::token_stream::GenericTokenParserRegistry;
// use crate::ssrp;  // TODO: Enable when SSRP implementation is added

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

    /// Create a client by parsing a data source string
    ///
    /// This method parses the data source string and attempts connection using
    /// the appropriate protocol. The existing TransportContext parsing logic
    /// will handle LocalDB resolution and protocol selection.
    ///
    /// # Arguments
    /// * `context` - The client context with connection parameters
    /// * `datasource` - The data source string (e.g., "tcp:server,1433", "server\instance")
    /// * `cancel_handle` - Optional cancellation handle
    ///
    /// # Returns
    /// A connected TdsClient
    /// Deprecated: Use create_client() instead.
    /// This method is kept for backward compatibility.
    #[deprecated(since = "0.1.0", note = "Use create_client() instead")]
    pub async fn create_client_from_datasource(
        &self,
        context: ClientContext,
        datasource: &str,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<TdsClient> {
        self.create_client(context, datasource, cancel_handle).await
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

    /// Create a client from a datasource string.
    /// This is the primary API for creating connections.
    /// 
    /// # Arguments
    /// * `context` - Client context with credentials and options (without transport_context set)
    /// * `datasource` - The data source string (e.g., "tcp:server,1433", "server\\instance", "lpc:.")
    /// * `cancel_handle` - Optional cancellation handle
    pub async fn create_client(
        &self,
        mut context: ClientContext,
        datasource: &str,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<TdsClient> {
        // Parse the datasource and populate transport_context
        let _parsed = context.parse_datasource(datasource)?;
        self.create_client_internal(context, cancel_handle).await
    }

    /// Internal method for creating clients with transport context already resolved.
    /// This is used internally after datasource parsing.
    pub(crate) async fn create_client_internal(
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

        // Determine if we need to build a protocol list or use explicit transport
        let transport_contexts = Self::resolve_transport_contexts(context).await?;

        debug!(
            "Resolved {} transport context(s) to try",
            transport_contexts.len()
        );

        let mut last_error = None;

        // Try each transport context in order
        for transport_ctx in &transport_contexts {
            debug!("Attempting connection with {:?}", transport_ctx);

            let mut connection_result =
                Self::connect_with_transport_context(context, transport_ctx).await;

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
                        debug!("Connection successful");
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
                            connection_result = Self::connect_with_transport_context(
                                context,
                                &tcp_transport_context,
                            )
                            .await;
                        }
                        _ => {
                            // Not a redirection error, save and try next protocol
                            debug!("Connection failed: {}", err);
                            last_error = Some(err);
                            break;
                        }
                    },
                }
            }
        }

        // All protocols failed, return the last error
        Err(last_error.unwrap_or_else(|| {
            Error::ProtocolError(
                "No transport protocols available or all connection attempts failed.".to_string(),
            )
        }))
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

    /// Resolve transport contexts based on connection parameters
    ///
    /// This implements ODBC-like protocol resolution:
    /// 1. If explicit transport_context exists → use it
    /// 2. Otherwise build default protocol list and optionally query SSRP
    ///
    /// Protocol order (for no explicit protocol):
    /// - SharedMemory (if local server)
    /// - TCP
    /// - NamedPipe (Windows only)
    async fn resolve_transport_contexts(
        context: &ClientContext,
    ) -> TdsResult<Vec<TransportContext>> {
        // Check if we need SSRP (instance name without explicit port)
        // This check must happen BEFORE protocol selection, because even with explicit protocol,
        // instance names require SSRP to resolve the port
        let needs_ssrp = Self::needs_ssrp_query(context);

        if needs_ssrp {
            debug!("Instance name detected without port, SSRP query needed");
            return Err(Error::ProtocolError(
                "Named instance connection requires SSRP (SQL Server Browser), which is not yet implemented. \
                 Please use explicit protocol with port: tcp:server,1433".to_string()
            ));
        }

        // Check if we have an explicit protocol specified (tcp:, np:, lpc:, admin:)
        // When a protocol is explicitly specified (via datasource string like "tcp:server,1433"
        // or "server,1433"), only try that single protocol.
        // Also, if a complete transport context was set directly (not via datasource parsing),
        // such as in tests or direct API usage, use it as-is without protocol fallback.
        let has_complete_transport = matches!(&context.transport_context,
            TransportContext::Tcp { port, .. } if *port != 0 && *port != 1433
        ) || {
            #[cfg(windows)]
            {
                matches!(
                    &context.transport_context,
                    TransportContext::LocalDB { .. }
                        | TransportContext::SharedMemory { .. }
                        | TransportContext::NamedPipe { .. }
                )
            }
            #[cfg(not(windows))]
            {
                // On non-Windows, only TCP is supported as a complete transport
                false
            }
        };

        if context.explicit_protocol || has_complete_transport {
            debug!(
                "Explicit protocol or complete transport specified, using single transport: {:?}",
                context.transport_context
            );
            return Ok(vec![context.transport_context.clone()]);
        }

        // No explicit protocol - build default protocol list
        debug!("No explicit protocol, building default protocol list");

        let server = context.transport_context.get_server_name();
        #[cfg_attr(not(windows), allow(unused_variables))]
        let is_local = Self::is_local_server(&server);

        let mut transports = Vec::new();

        // Build default protocol list based on ODBC precedence

        // 1. Shared Memory (only for local connections)
        #[cfg(windows)]
        if is_local {
            debug!("Adding SharedMemory to protocol list (local server)");
            transports.push(TransportContext::SharedMemory {
                instance_name: String::new(), // Default instance
            });
        }

        // 2. TCP (always available)
        match &context.transport_context {
            TransportContext::Tcp { host, port } => {
                debug!("Adding TCP to protocol list: {}:{}", host, port);
                transports.push(TransportContext::Tcp {
                    host: host.clone(),
                    port: *port,
                });
            }
            _ => {
                // Default TCP port if no port specified
                debug!("Adding TCP with default port 1433");
                transports.push(TransportContext::Tcp {
                    host: server.clone(),
                    port: 1433,
                });
            }
        }

        // 3. Named Pipes (Windows only, not for LocalDB)
        #[cfg(windows)]
        if !matches!(context.transport_context, TransportContext::LocalDB { .. }) {
            debug!("Adding Named Pipe to protocol list");
            // Standard default instance pipe
            let pipe_name = if is_local {
                r"\\.\pipe\sql\query".to_string()
            } else {
                format!(r"\\{}\pipe\sql\query", server)
            };
            transports.push(TransportContext::NamedPipe { pipe_name });
        }

        if transports.is_empty() {
            return Err(Error::ProtocolError(
                "No client protocols are enabled and no protocol was specified in the connection string".to_string()
            ));
        }

        debug!("Built protocol list with {} transport(s)", transports.len());
        Ok(transports)
    }

    /// Check if server name refers to localhost
    fn is_local_server(server: &str) -> bool {
        let server_lower = server.to_lowercase();
        matches!(
            server_lower.as_str(),
            "." | "localhost" | "127.0.0.1" | "::1" | "(local)"
        ) || server_lower.starts_with("(localdb)")
    }

    /// Check if SSRP query is needed
    /// SSRP is required when:
    /// - No explicit port specified AND
    /// - Instance name is present (not default instance)
    fn needs_ssrp_query(context: &ClientContext) -> bool {
        // Check if we have an instance name
        let has_instance = !context.database_instance.is_empty()
            && context.database_instance != "MSSQLServer"
            && context.database_instance != "MSSQLSERVER";

        if !has_instance {
            return false;
        }

        // Check if we have an explicit port (which means we don't need SSRP)
        let has_explicit_port = matches!(&context.transport_context, TransportContext::Tcp { port, .. } if *port != 1433);

        // Need SSRP if we have an instance but no explicit port
        has_instance && !has_explicit_port
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
