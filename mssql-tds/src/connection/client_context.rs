// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use async_trait::async_trait;

use crate::connection::datasource_parser::{ParsedDataSource, ProtocolType};
use crate::core::{EncryptionOptions, EncryptionSetting, TdsResult};
use crate::message::login_options::{ApplicationIntent, TdsVersion};
use hostname;

#[derive(PartialEq, Copy, Clone)]
pub enum IPAddressPreference {
    IPv4First = 0,
    IPv6First = 1,
    UsePlatformDefault = 2,
}

/// Specifies the Vector feature version support level.
#[derive(PartialEq, Copy, Clone, Debug)]
pub enum VectorVersion {
    /// Vector support is disabled
    Off,
    /// Support Vector feature version 1 (float32 dimension type)
    V1,
}

/// Provides a trait for creating Entra ID tokens.
#[async_trait]
pub trait EntraIdTokenFactory: Send + Sync {
    async fn create_token(
        &self,
        spn: String,
        sts_url: String,
        auth_method: TdsAuthenticationMethod,
    ) -> TdsResult<Vec<u8>>;
}
pub trait CloneableEntraIdTokenFactory: EntraIdTokenFactory {
    fn clone_box(&self) -> Box<dyn CloneableEntraIdTokenFactory>;
}

impl<T> CloneableEntraIdTokenFactory for T
where
    T: EntraIdTokenFactory + Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn CloneableEntraIdTokenFactory> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub enum TdsAuthenticationMethod {
    Password,
    SSPI, // Integrated Authentication with AD.
    ActiveDirectoryPassword,
    ActiveDirectoryInteractive,
    ActiveDirectoryDeviceCodeFlow,
    ActiveDirectoryServicePrincipal,
    ActiveDirectoryManagedIdentity,
    ActiveDirectoryDefault,
    ActiveDirectoryMSI,
    ActiveDirectoryWorkloadIdentity,
    ActiveDirectoryIntegrated,
    AccessToken,
}

use std::collections::HashMap;

pub struct ClientContext {
    pub application_intent: ApplicationIntent,
    pub application_name: String,
    pub attach_db_file: String,
    pub change_password: String,
    pub connect_retry_count: u32,
    pub connect_timeout: u32,
    pub database: String,
    /// The original data source string used to create this connection.
    /// This is a mandatory field - a connection cannot be established without it.
    /// Examples: "tcp:myserver,1433", "myserver\instance", "lpc:."
    pub data_source: String,
    /// TCP keep-alive idle time in milliseconds before first probe is sent.
    /// Default: 30000 (30 seconds) per SQL Server client defaults.
    /// Named to match ODBC Driver's "KeepAlive" connection string parameter.
    pub keep_alive_in_ms: u32,
    /// TCP keep-alive interval in milliseconds between subsequent probes.
    /// Default: 1000 (1 second) per SQL Server client defaults.
    pub keep_alive_interval_in_ms: u32,
    pub database_instance: String,
    pub enlist: bool,
    pub encryption_options: EncryptionOptions,
    pub failover_partner: String,
    pub ipaddress_preference: IPAddressPreference,
    pub language: String,
    pub library_name: String,
    pub auth_method_map: HashMap<TdsAuthenticationMethod, Box<dyn CloneableEntraIdTokenFactory>>,
    pub mars_enabled: bool,
    pub multi_subnet_failover: bool,
    pub new_password: String,
    pub packet_size: i16,
    pub password: String,
    pub pooling: bool,
    pub replication: bool,
    pub tds_authentication_method: TdsAuthenticationMethod,
    pub user_instance: bool,
    pub user_name: String,
    pub workstation_id: String,
    pub server_spn: Option<String>,
    pub access_token: Option<String>,
    pub(crate) transport_context: TransportContext,
    pub vector_version: VectorVersion,
}

impl ClientContext {
    /// Creates a new ClientContext with the specified data source.
    /// The data source is mandatory for establishing a connection.
    ///
    /// # Arguments
    /// * `data_source` - The data source string (e.g., "tcp:myserver,1433", "myserver\\instance")
    ///
    /// # Example
    /// ```
    /// let context = ClientContext::with_data_source("tcp:myserver,1433");
    /// ```
    pub fn with_data_source(data_source: &str) -> ClientContext {
        ClientContext {
            application_intent: ApplicationIntent::ReadWrite,
            application_name: "TDSX Rust Client".to_string(),
            attach_db_file: "".to_string(),
            change_password: "".to_string(),
            connect_retry_count: 0,
            connect_timeout: 15,
            database: "".to_string(),
            data_source: data_source.to_string(),
            keep_alive_in_ms: 30_000, // 30 seconds (SQL Server default)
            keep_alive_interval_in_ms: 1_000, // 1 second (SQL Server default)
            database_instance: "MSSQLServer".to_string(),
            enlist: false,
            encryption_options: EncryptionOptions::new(),
            failover_partner: "".to_string(),
            ipaddress_preference: IPAddressPreference::UsePlatformDefault,
            language: "us_english".to_string(),
            library_name: "TdsX".to_string(),
            auth_method_map: HashMap::new(),
            mars_enabled: false,
            multi_subnet_failover: false,
            new_password: "".to_string(),
            packet_size: 8000,
            password: "".to_string(),
            pooling: false,
            replication: false,
            server_spn: None,
            tds_authentication_method: TdsAuthenticationMethod::Password,
            user_instance: false,
            user_name: "".to_string(),
            workstation_id: ClientContext::default_workstation_id(hostname::get),
            access_token: None,
            transport_context: TransportContext::Tcp {
                host: "localhost".to_string(),
                port: 1433,
            },
            vector_version: VectorVersion::V1,
        }
    }

    /// Creates a new ClientContext with default values.
    /// Note: The data_source field will be empty and must be set before connecting,
    /// either directly or by calling parse_datasource().
    ///
    /// Consider using `with_data_source()` instead for clearer intent.
    #[deprecated(
        since = "0.2.0",
        note = "Use with_data_source() instead for clearer intent"
    )]
    pub fn new() -> ClientContext {
        ClientContext {
            application_intent: ApplicationIntent::ReadWrite,
            application_name: "TDSX Rust Client".to_string(),
            attach_db_file: "".to_string(),
            change_password: "".to_string(),
            connect_retry_count: 0,
            connect_timeout: 15,
            database: "".to_string(),
            data_source: "".to_string(),
            keep_alive_in_ms: 30_000, // 30 seconds (SQL Server default)
            keep_alive_interval_in_ms: 1_000, // 1 second (SQL Server default)
            database_instance: "MSSQLServer".to_string(),
            enlist: false,
            encryption_options: EncryptionOptions::new(),
            failover_partner: "".to_string(),
            ipaddress_preference: IPAddressPreference::UsePlatformDefault,
            language: "us_english".to_string(),
            library_name: "TdsX".to_string(),
            auth_method_map: HashMap::new(),
            mars_enabled: false,
            multi_subnet_failover: false,
            new_password: "".to_string(),
            packet_size: 8000,
            password: "".to_string(),
            pooling: false,
            replication: false,
            tds_authentication_method: TdsAuthenticationMethod::Password,
            user_instance: false,
            user_name: "".to_string(),
            workstation_id: ClientContext::default_workstation_id(hostname::get),
            server_spn: None,
            access_token: None,
            transport_context: TransportContext::Tcp {
                host: "localhost".to_string(),
                port: 1433,
            },
            vector_version: VectorVersion::V1,
        }
    }

    pub fn integrated_security(&self) -> bool {
        matches!(
            self.tds_authentication_method,
            TdsAuthenticationMethod::SSPI
        )
    }

    pub fn tds_version(&self) -> TdsVersion {
        if matches!(self.encryption_options.mode, EncryptionSetting::Strict) {
            TdsVersion::V8_0
        } else {
            TdsVersion::V7_4
        }
    }

    fn clone_auth_method_map(
        &self,
    ) -> HashMap<TdsAuthenticationMethod, Box<dyn CloneableEntraIdTokenFactory>> {
        self.auth_method_map
            .iter()
            .map(|(key, value)| (key.clone(), value.clone_box()))
            .collect()
    }
}

impl Default for ClientContext {
    #[allow(deprecated)]
    fn default() -> Self {
        Self::new()
    }
}

impl ClientContext {
    /// Generates a default workstation ID based on the hostname.
    /// If the hostname is longer than 128 characters, it truncates it to 128 characters.
    /// This function is used to ensure that the workstation ID does not exceed the maximum length
    /// allowed by the server.
    fn default_workstation_id<F>(get_hostname: F) -> String
    where
        F: Fn() -> Result<std::ffi::OsString, std::io::Error>,
    {
        let hostname = get_hostname()
            .unwrap_or_else(|_| "".into())
            .to_string_lossy()
            .to_string();
        if hostname.len() > 128 {
            hostname[..128].to_string()
        } else {
            hostname
        }
    }

    /// Parse a data source string and update the ClientContext with the parsed transport
    ///
    /// This method parses the data source string (e.g., "tcp:server,1433", "server\instance")
    /// and updates the transport_context field of the ClientContext.
    /// It also stores the original data source string for logging and diagnostics.
    ///
    /// # Arguments
    /// * `datasource` - The data source string to parse
    ///
    /// # Returns
    /// A Result containing the parsed data source information
    ///
    /// # Example
    /// ```
    /// let mut context = ClientContext::new();
    /// let parsed = context.parse_datasource("tcp:myserver,1433")?;
    /// ```
    pub fn parse_datasource(&mut self, datasource: &str) -> TdsResult<ParsedDataSource> {
        let parsed = ParsedDataSource::parse(datasource, false)?;

        // Store the original data source string
        self.data_source = datasource.to_string();

        // Update transport context based on parsed data source
        self.transport_context = TransportContext::from_parsed_datasource(&parsed)?;

        // Store instance name for protocol resolution
        if !parsed.instance_name.is_empty() {
            self.database_instance = parsed.instance_name.clone();
        }

        Ok(parsed)
    }

    /// Parse a data source string with MultiSubnetFailover support
    ///
    /// Similar to parse_datasource but allows specifying MultiSubnetFailover option
    /// which restricts protocol selection to TCP only.
    ///
    /// # Arguments
    /// * `datasource` - The data source string to parse
    /// * `multi_subnet_failover` - Whether MultiSubnetFailover is enabled
    pub fn parse_datasource_with_options(
        &mut self,
        datasource: &str,
        multi_subnet_failover: bool,
    ) -> TdsResult<ParsedDataSource> {
        let parsed = ParsedDataSource::parse(datasource, multi_subnet_failover)?;

        // Store the original data source string
        self.data_source = datasource.to_string();

        // Update transport context based on parsed data source
        self.transport_context = TransportContext::from_parsed_datasource(&parsed)?;

        // Store instance name for protocol resolution
        if !parsed.instance_name.is_empty() {
            self.database_instance = parsed.instance_name.clone();
        }

        Ok(parsed)
    }
}

impl Clone for ClientContext {
    fn clone(&self) -> Self {
        ClientContext {
            application_intent: self.application_intent,
            application_name: self.application_name.clone(),
            attach_db_file: self.attach_db_file.clone(),
            change_password: self.change_password.clone(),
            connect_retry_count: self.connect_retry_count,
            connect_timeout: self.connect_timeout,
            database: self.database.clone(),
            data_source: self.data_source.clone(),
            keep_alive_in_ms: self.keep_alive_in_ms,
            keep_alive_interval_in_ms: self.keep_alive_interval_in_ms,
            database_instance: self.database_instance.clone(),
            enlist: self.enlist,
            encryption_options: self.encryption_options.clone(),
            failover_partner: self.failover_partner.clone(),
            ipaddress_preference: self.ipaddress_preference,
            language: self.language.clone(),
            library_name: self.library_name.clone(),
            auth_method_map: self.clone_auth_method_map(),
            mars_enabled: self.mars_enabled,
            multi_subnet_failover: self.multi_subnet_failover,
            new_password: self.new_password.clone(),
            packet_size: self.packet_size,
            password: self.password.clone(),
            pooling: self.pooling,
            replication: self.replication,
            tds_authentication_method: self.tds_authentication_method.clone(),
            user_instance: self.user_instance,
            user_name: self.user_name.clone(),
            workstation_id: self.workstation_id.clone(),
            server_spn: self.server_spn.clone(),
            access_token: self.access_token.clone(),
            transport_context: self.transport_context.clone(),
            vector_version: self.vector_version,
        }
    }
}

/// Protocol types for SQL Server connections
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// TCP/IP protocol (default)
    Tcp,
    /// Named Pipes protocol
    NamedPipe,
    /// Shared Memory protocol (local only)
    SharedMemory,
}

#[derive(PartialEq, Clone, Debug)]
pub enum TransportContext {
    /// TCP/IP connection with host and port
    Tcp { host: String, port: u16 },
    /// Named Pipe connection with pipe path
    /// Format: \\server\pipe\sql\query or \\server\pipe\MSSQL$INSTANCE\sql\query
    NamedPipe { pipe_name: String },
    /// Shared Memory connection (local only) with optional instance name
    SharedMemory { instance_name: String },
    /// LocalDB connection (Windows only) with instance name
    /// Format: (localdb)\InstanceName
    #[cfg(windows)]
    LocalDB { instance_name: String },
}

impl TransportContext {
    /// Get the server name from the transport context
    pub fn get_server_name(&self) -> String {
        match self {
            TransportContext::Tcp { host, .. } => host.clone(),
            TransportContext::NamedPipe { pipe_name } => {
                // Extract server name from pipe path like \\.\pipe\sql\query or \\server\pipe\sql\query
                if pipe_name.starts_with("\\\\.\\") {
                    "localhost".to_string()
                } else if let Some(rest) = pipe_name.strip_prefix("\\\\") {
                    if let Some(idx) = rest.find('\\') {
                        rest[..idx].to_string()
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                }
            }
            TransportContext::SharedMemory { .. } => "localhost".to_string(),
            #[cfg(windows)]
            TransportContext::LocalDB { instance_name } => {
                format!("(localdb)\\{instance_name}")
            }
        }
    }

    /// Get the protocol type for this transport context
    pub fn get_protocol(&self) -> Protocol {
        match self {
            TransportContext::Tcp { .. } => Protocol::Tcp,
            TransportContext::NamedPipe { .. } => Protocol::NamedPipe,
            TransportContext::SharedMemory { .. } => Protocol::SharedMemory,
            #[cfg(windows)]
            TransportContext::LocalDB { .. } => Protocol::NamedPipe, // LocalDB uses named pipes internally
        }
    }

    /// Check if the connection is local
    pub fn is_local(&self) -> bool {
        match self {
            TransportContext::Tcp { host, .. } => {
                matches!(
                    host.to_lowercase().as_str(),
                    "." | "(local)" | "localhost" | "127.0.0.1" | "::1"
                )
            }
            TransportContext::NamedPipe { pipe_name } => pipe_name.starts_with("\\\\.\\"),
            TransportContext::SharedMemory { .. } => true,
            #[cfg(windows)]
            TransportContext::LocalDB { .. } => true, // LocalDB is always local
        }
    }

    /// Create TransportContext from a parsed data source
    ///
    /// This method converts a ParsedDataSource into a TransportContext,
    /// determining the appropriate transport method based on the parsed data.
    pub fn from_parsed_datasource(parsed: &ParsedDataSource) -> TdsResult<Self> {
        use crate::error::Error;

        match parsed.get_protocol_type() {
            ProtocolType::Tcp => {
                let port = if !parsed.protocol_parameter.is_empty() {
                    parsed
                        .protocol_parameter
                        .parse::<u16>()
                        .map_err(|e| Error::ProtocolError(format!("Invalid port number: {}", e)))?
                } else {
                    1433 // Default SQL Server port
                };

                Ok(TransportContext::Tcp {
                    host: parsed.server_name.clone(),
                    port,
                })
            }
            ProtocolType::NamedPipe => {
                let pipe_name = if !parsed.protocol_parameter.is_empty() {
                    parsed.protocol_parameter.clone()
                } else if !parsed.instance_name.is_empty() {
                    // Build standard named pipe path
                    if parsed.instance_name.to_lowercase() == "default"
                        || parsed.instance_name.is_empty()
                    {
                        format!("\\\\{}\\pipe\\sql\\query", parsed.server_name)
                    } else {
                        format!(
                            "\\\\{}\\pipe\\MSSQL${}\\sql\\query",
                            parsed.server_name, parsed.instance_name
                        )
                    }
                } else {
                    // Default instance
                    format!("\\\\{}\\pipe\\sql\\query", parsed.server_name)
                };

                Ok(TransportContext::NamedPipe { pipe_name })
            }
            ProtocolType::SharedMemory => {
                let instance_name = if !parsed.instance_name.is_empty() {
                    parsed.instance_name.clone()
                } else {
                    "MSSQLSERVER".to_string()
                };

                Ok(TransportContext::SharedMemory { instance_name })
            }
            ProtocolType::Admin => {
                // DAC always uses TCP on port 1434 by default
                Ok(TransportContext::Tcp {
                    host: parsed.server_name.clone(),
                    port: 1434,
                })
            }
            ProtocolType::Auto => {
                // Auto-detect: prefer TCP with default port
                Ok(TransportContext::Tcp {
                    host: parsed.server_name.clone(),
                    port: 1433,
                })
            }
        }
    }

    /// Parse a server name string and return the appropriate TransportContext
    ///
    /// Supported formats:
    /// - `(localdb)\InstanceName` or `(localdb)/InstanceName` -> LocalDB (Windows only)
    /// - `\\server\pipe\path` -> NamedPipe
    /// - `lpc:InstanceName` -> SharedMemory
    /// - `hostname:port` -> Tcp
    /// - `hostname` -> Tcp with default_port
    pub fn parse_server_name(server_name: &str, default_port: u16) -> TransportContext {
        let server_lower = server_name.to_lowercase();

        // Check for LocalDB format: (localdb)\InstanceName or (localdb)/InstanceName
        #[cfg(windows)]
        if server_lower.starts_with("(localdb)\\") || server_lower.starts_with("(localdb)/") {
            let instance_name = server_name[10..].to_string(); // Skip "(localdb)\" or "(localdb)/"
            return TransportContext::LocalDB { instance_name };
        }

        // Check for Named Pipe format: \\server\pipe\...
        if server_name.starts_with("\\\\") {
            return TransportContext::NamedPipe {
                pipe_name: server_name.to_string(),
            };
        }

        // Check for Shared Memory format: lpc:InstanceName
        if server_lower.starts_with("lpc:") {
            let instance_name = server_name[4..].to_string(); // Skip "lpc:"
            return TransportContext::SharedMemory { instance_name };
        }

        // Parse TCP format: hostname or hostname,port
        // SQL Server connection strings use comma as the port separator (e.g., localhost,1433)
        if let Some(comma_idx) = server_name.rfind(',') {
            let host = server_name[..comma_idx].to_string();
            let port_str = &server_name[comma_idx + 1..];
            if let Ok(port) = port_str.parse::<u16>() {
                return TransportContext::Tcp { host, port };
            }
        }

        // Default: treat as hostname without port
        TransportContext::Tcp {
            host: server_name.to_string(),
            port: default_port,
        }
    }

    /// Check if this is a LocalDB connection (Windows only)
    #[cfg(windows)]
    pub fn is_localdb(&self) -> bool {
        matches!(self, TransportContext::LocalDB { .. })
    }

    /// Get the LocalDB instance name if this is a LocalDB connection (Windows only)
    #[cfg(windows)]
    pub fn get_localdb_instance(&self) -> Option<&str> {
        match self {
            TransportContext::LocalDB { instance_name } => Some(instance_name.as_str()),
            _ => None,
        }
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;

    #[test]
    fn test_with_data_source_constructor() {
        let ctx = ClientContext::with_data_source("tcp:myserver,1433");
        assert_eq!(ctx.data_source, "tcp:myserver,1433");
        // Other defaults should still be set
        assert_eq!(ctx.connect_timeout, 15);
        assert_eq!(ctx.packet_size, 8000);
        assert_eq!(ctx.application_name, "TDSX Rust Client");
    }

    #[test]
    fn test_parse_datasource_sets_data_source() {
        let mut ctx = ClientContext::new();
        assert_eq!(ctx.data_source, ""); // Initially empty

        let _ = ctx.parse_datasource("tcp:myserver,1433");
        assert_eq!(ctx.data_source, "tcp:myserver,1433");
    }

    #[test]
    fn test_data_source_cloned() {
        let ctx = ClientContext::with_data_source("tcp:myserver,1433");
        let cloned = ctx.clone();
        assert_eq!(cloned.data_source, "tcp:myserver,1433");
    }

    #[test]
    fn test_default_workstation_id_truncation() {
        // Simulate a long hostname
        let long_hostname = "a".repeat(150);
        let truncated_hostname = long_hostname[..128].to_string();

        // Test the default_workstation_id function with a mock closure
        let result = ClientContext::default_workstation_id(|| {
            Ok(std::ffi::OsString::from(long_hostname.clone()))
        });
        assert_eq!(result, truncated_hostname);
    }

    #[test]
    fn test_tcp_transport_get_server_name() {
        let ctx = TransportContext::Tcp {
            host: "myserver.example.com".to_string(),
            port: 1433,
        };
        assert_eq!(ctx.get_server_name(), "myserver.example.com");
        assert_eq!(ctx.get_protocol(), Protocol::Tcp);
        assert!(!ctx.is_local());
    }

    #[test]
    fn test_tcp_transport_localhost() {
        let ctx = TransportContext::Tcp {
            host: "localhost".to_string(),
            port: 1433,
        };
        assert_eq!(ctx.get_server_name(), "localhost");
        assert!(ctx.is_local());
    }

    #[test]
    fn test_named_pipe_local() {
        let ctx = TransportContext::NamedPipe {
            pipe_name: "\\\\.\\pipe\\sql\\query".to_string(),
        };
        assert_eq!(ctx.get_server_name(), "localhost");
        assert_eq!(ctx.get_protocol(), Protocol::NamedPipe);
        assert!(ctx.is_local());
    }

    #[test]
    fn test_named_pipe_remote() {
        let ctx = TransportContext::NamedPipe {
            pipe_name: "\\\\myserver\\pipe\\sql\\query".to_string(),
        };
        assert_eq!(ctx.get_server_name(), "myserver");
        assert_eq!(ctx.get_protocol(), Protocol::NamedPipe);
        assert!(!ctx.is_local());
    }

    #[test]
    fn test_named_pipe_with_instance() {
        let ctx = TransportContext::NamedPipe {
            pipe_name: "\\\\myserver\\pipe\\MSSQL$SQLEXPRESS\\sql\\query".to_string(),
        };
        assert_eq!(ctx.get_server_name(), "myserver");
        assert_eq!(ctx.get_protocol(), Protocol::NamedPipe);
    }

    #[test]
    fn test_shared_memory() {
        let ctx = TransportContext::SharedMemory {
            instance_name: "MSSQLSERVER".to_string(),
        };
        assert_eq!(ctx.get_server_name(), "localhost");
        assert_eq!(ctx.get_protocol(), Protocol::SharedMemory);
        assert!(ctx.is_local());
    }

    #[test]
    fn test_shared_memory_with_instance() {
        let ctx = TransportContext::SharedMemory {
            instance_name: "SQLEXPRESS".to_string(),
        };
        assert_eq!(ctx.get_server_name(), "localhost");
        assert!(ctx.is_local());
    }

    #[test]
    fn test_transport_context_get_server_name() {
        // TCP
        let tcp_context = TransportContext::Tcp {
            host: "localhost".to_string(),
            port: 1433,
        };
        assert_eq!(tcp_context.get_server_name(), "localhost");

        // Named Pipe
        let np_context = TransportContext::NamedPipe {
            pipe_name: r"\\server\pipe\sql\query".to_string(),
        };
        assert_eq!(np_context.get_server_name(), "server");

        let np_local_context = TransportContext::NamedPipe {
            pipe_name: r"\\.\pipe\sql\query".to_string(),
        };
        assert_eq!(np_local_context.get_server_name(), "localhost");

        // Shared Memory
        let sm_context = TransportContext::SharedMemory {
            instance_name: String::new(),
        };
        assert_eq!(sm_context.get_server_name(), "localhost");

        let sm_named_context = TransportContext::SharedMemory {
            instance_name: "SQLEXPRESS".to_string(),
        };
        assert_eq!(sm_named_context.get_server_name(), "localhost");
    }

    #[test]
    fn test_transport_context_is_local() {
        // TCP - not local
        let tcp_context = TransportContext::Tcp {
            host: "remote-server".to_string(),
            port: 1433,
        };
        assert!(!tcp_context.is_local());

        // TCP - localhost
        let tcp_localhost = TransportContext::Tcp {
            host: "localhost".to_string(),
            port: 1433,
        };
        assert!(tcp_localhost.is_local());

        // TCP - 127.0.0.1
        let tcp_loopback = TransportContext::Tcp {
            host: "127.0.0.1".to_string(),
            port: 1433,
        };
        assert!(tcp_loopback.is_local());

        // Named Pipe with . (local)
        let np_local = TransportContext::NamedPipe {
            pipe_name: r"\\.\pipe\sql\query".to_string(),
        };
        assert!(np_local.is_local());

        // Named Pipe with remote server
        let np_remote = TransportContext::NamedPipe {
            pipe_name: r"\\remote-server\pipe\sql\query".to_string(),
        };
        assert!(!np_remote.is_local());

        // Shared Memory - always local
        let sm_context = TransportContext::SharedMemory {
            instance_name: String::new(),
        };
        assert!(sm_context.is_local());
    }

    #[test]
    fn test_transport_context_get_protocol() {
        // TCP
        let tcp_context = TransportContext::Tcp {
            host: "localhost".to_string(),
            port: 1433,
        };
        assert!(matches!(tcp_context.get_protocol(), Protocol::Tcp));

        // Named Pipe
        let np_context = TransportContext::NamedPipe {
            pipe_name: r"\\.\pipe\sql\query".to_string(),
        };
        assert!(matches!(np_context.get_protocol(), Protocol::NamedPipe));

        // Shared Memory
        let sm_context = TransportContext::SharedMemory {
            instance_name: String::new(),
        };
        assert!(matches!(sm_context.get_protocol(), Protocol::SharedMemory));
    }

    // LocalDB parsing tests
    #[test]
    #[cfg(windows)]
    fn test_parse_server_name_localdb() {
        // Test basic LocalDB format with backslash
        let ctx = TransportContext::parse_server_name("(localdb)\\MSSQLLocalDB", 1433);
        assert!(ctx.is_localdb());
        assert_eq!(ctx.get_localdb_instance(), Some("MSSQLLocalDB"));
        assert_eq!(ctx.get_server_name(), "(localdb)\\MSSQLLocalDB");
        assert!(ctx.is_local());
        assert_eq!(ctx.get_protocol(), Protocol::NamedPipe);

        // Test LocalDB with forward slash
        let ctx2 = TransportContext::parse_server_name("(localdb)/v11.0", 1433);
        assert!(ctx2.is_localdb());
        assert_eq!(ctx2.get_localdb_instance(), Some("v11.0"));

        // Test case insensitivity
        let ctx3 = TransportContext::parse_server_name("(LocalDB)\\MyInstance", 1433);
        assert!(ctx3.is_localdb());
        assert_eq!(ctx3.get_localdb_instance(), Some("MyInstance"));

        // Test with uppercase
        let ctx4 = TransportContext::parse_server_name("(LOCALDB)\\TEST", 1433);
        assert!(ctx4.is_localdb());
        assert_eq!(ctx4.get_localdb_instance(), Some("TEST"));
    }

    #[test]
    fn test_parse_server_name_tcp() {
        // Simple hostname
        let ctx = TransportContext::parse_server_name("myserver", 1433);
        assert_eq!(ctx.get_server_name(), "myserver");
        assert_eq!(ctx.get_protocol(), Protocol::Tcp);
        if let TransportContext::Tcp { host, port } = ctx {
            assert_eq!(host, "myserver");
            assert_eq!(port, 1433);
        } else {
            panic!("Expected Tcp variant");
        }

        // Hostname with port (SQL Server uses comma as separator)
        let ctx2 = TransportContext::parse_server_name("myserver,1434", 1433);
        if let TransportContext::Tcp { host, port } = ctx2 {
            assert_eq!(host, "myserver");
            assert_eq!(port, 1434);
        } else {
            panic!("Expected Tcp variant");
        }

        // Hostname with domain
        let ctx3 = TransportContext::parse_server_name("sql.contoso.com", 1433);
        if let TransportContext::Tcp { host, port } = ctx3 {
            assert_eq!(host, "sql.contoso.com");
            assert_eq!(port, 1433);
        } else {
            panic!("Expected Tcp variant");
        }

        // IP address with port
        let ctx4 = TransportContext::parse_server_name("192.168.1.100,5000", 1433);
        if let TransportContext::Tcp { host, port } = ctx4 {
            assert_eq!(host, "192.168.1.100");
            assert_eq!(port, 5000);
        } else {
            panic!("Expected Tcp variant");
        }

        // localhost
        let ctx5 = TransportContext::parse_server_name("localhost", 1433);
        assert!(ctx5.is_local());
    }

    #[test]
    fn test_parse_server_name_named_pipe() {
        // Local named pipe
        let ctx = TransportContext::parse_server_name("\\\\.\\pipe\\sql\\query", 1433);
        assert_eq!(ctx.get_protocol(), Protocol::NamedPipe);
        assert_eq!(ctx.get_server_name(), "localhost");
        assert!(ctx.is_local());
        if let TransportContext::NamedPipe { pipe_name } = ctx {
            assert_eq!(pipe_name, "\\\\.\\pipe\\sql\\query");
        } else {
            panic!("Expected NamedPipe variant");
        }

        // Remote named pipe
        let ctx2 = TransportContext::parse_server_name("\\\\server\\pipe\\sql\\query", 1433);
        assert_eq!(ctx2.get_server_name(), "server");
        assert!(!ctx2.is_local());

        // Named pipe with instance
        let ctx3 = TransportContext::parse_server_name(
            "\\\\server\\pipe\\MSSQL$SQLEXPRESS\\sql\\query",
            1433,
        );
        assert_eq!(ctx3.get_server_name(), "server");
        if let TransportContext::NamedPipe { pipe_name } = ctx3 {
            assert_eq!(pipe_name, "\\\\server\\pipe\\MSSQL$SQLEXPRESS\\sql\\query");
        } else {
            panic!("Expected NamedPipe variant");
        }
    }

    #[test]
    fn test_parse_server_name_shared_memory() {
        // Shared memory default instance
        let ctx = TransportContext::parse_server_name("lpc:MSSQLSERVER", 1433);
        assert_eq!(ctx.get_protocol(), Protocol::SharedMemory);
        assert!(ctx.is_local());
        if let TransportContext::SharedMemory { instance_name } = ctx {
            assert_eq!(instance_name, "MSSQLSERVER");
        } else {
            panic!("Expected SharedMemory variant");
        }

        // Shared memory named instance
        let ctx2 = TransportContext::parse_server_name("lpc:SQLEXPRESS", 1433);
        if let TransportContext::SharedMemory { instance_name } = ctx2 {
            assert_eq!(instance_name, "SQLEXPRESS");
        } else {
            panic!("Expected SharedMemory variant");
        }

        // Case insensitive
        let ctx3 = TransportContext::parse_server_name("LPC:MyInstance", 1433);
        assert_eq!(ctx3.get_protocol(), Protocol::SharedMemory);
    }

    #[test]
    #[cfg(windows)]
    fn test_localdb_helper_methods() {
        let localdb_ctx = TransportContext::LocalDB {
            instance_name: "TestInstance".to_string(),
        };
        assert!(localdb_ctx.is_localdb());
        assert_eq!(localdb_ctx.get_localdb_instance(), Some("TestInstance"));
        assert!(localdb_ctx.is_local());
        assert_eq!(localdb_ctx.get_protocol(), Protocol::NamedPipe);

        let tcp_ctx = TransportContext::Tcp {
            host: "localhost".to_string(),
            port: 1433,
        };
        assert!(!tcp_ctx.is_localdb());
        assert_eq!(tcp_ctx.get_localdb_instance(), None);
    }

    #[test]
    fn test_parse_special_cases() {
        // Dot notation (local)
        let ctx = TransportContext::parse_server_name(".", 1433);
        if let TransportContext::Tcp { host, .. } = &ctx {
            assert_eq!(host, ".");
            assert!(ctx.is_local());
        } else {
            panic!("Expected Tcp variant");
        }

        // (local) notation
        let ctx2 = TransportContext::parse_server_name("(local)", 1433);
        assert!(ctx2.is_local());

        // IPv6 loopback
        let ctx3 = TransportContext::parse_server_name("::1", 1433);
        assert!(ctx3.is_local());
    }

    #[test]
    fn test_client_context_keep_alive_defaults() {
        let ctx = ClientContext::new();
        // Default keep_alive_in_ms should be 30 seconds (30000 ms) per SQL Server defaults
        assert_eq!(ctx.keep_alive_in_ms, 30_000);
        // Default keep_alive_interval_in_ms should be 1 second (1000 ms) per SQL Server defaults
        assert_eq!(ctx.keep_alive_interval_in_ms, 1_000);
    }

    #[test]
    fn test_client_context_keep_alive_custom_values() {
        let mut ctx = ClientContext::new();
        ctx.keep_alive_in_ms = 60_000; // 60 seconds
        ctx.keep_alive_interval_in_ms = 5_000; // 5 seconds

        assert_eq!(ctx.keep_alive_in_ms, 60_000);
        assert_eq!(ctx.keep_alive_interval_in_ms, 5_000);
    }

    #[test]
    fn test_client_context_keep_alive_clone() {
        let mut ctx = ClientContext::new();
        ctx.keep_alive_in_ms = 45_000;
        ctx.keep_alive_interval_in_ms = 2_000;

        let cloned = ctx.clone();
        assert_eq!(cloned.keep_alive_in_ms, 45_000);
        assert_eq!(cloned.keep_alive_interval_in_ms, 2_000);
    }

    #[test]
    fn test_client_context_keep_alive_zero_values() {
        // Test that zero values are allowed (disables keep-alive on some systems)
        let mut ctx = ClientContext::new();
        ctx.keep_alive_in_ms = 0;
        ctx.keep_alive_interval_in_ms = 0;

        assert_eq!(ctx.keep_alive_in_ms, 0);
        assert_eq!(ctx.keep_alive_interval_in_ms, 0);
    }

    #[test]
    fn test_client_context_keep_alive_max_values() {
        // Test maximum u32 values
        let mut ctx = ClientContext::new();
        ctx.keep_alive_in_ms = u32::MAX;
        ctx.keep_alive_interval_in_ms = u32::MAX;

        assert_eq!(ctx.keep_alive_in_ms, u32::MAX);
        assert_eq!(ctx.keep_alive_interval_in_ms, u32::MAX);
    }
}
