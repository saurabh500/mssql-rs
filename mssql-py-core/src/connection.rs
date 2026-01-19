// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use mssql_tds::{
    connection::client_context::{
        ClientContext, IPAddressPreference, TdsAuthenticationMethod, TransportContext,
    },
    connection::tds_client::TdsClient,
    connection_provider::tds_connection_provider::TdsConnectionProvider,
    core::{EncryptionOptions, EncryptionSetting},
    message::login_options::ApplicationIntent,
};

/// Python Connection class for Core TDS backend
#[pyclass]
pub struct PyCoreConnection {
    #[allow(dead_code)] // Used for async operations in cursor execute
    runtime: Runtime,
    tds_client: Option<Arc<Mutex<TdsClient>>>,
    is_closed: bool,
}

#[pymethods]
impl PyCoreConnection {
    #[new]
    fn new(client_context_dict: &Bound<'_, PyDict>) -> PyResult<Self> {
        let runtime = Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {e}")))?;

        // Convert PyDict to ClientContext
        let client_context = Self::dict_to_client_context(client_context_dict)?;

        // Connect using TdsConnectionProvider
        let provider = TdsConnectionProvider {};
        let tds_client =
            runtime.block_on(async { provider.create_client(client_context, None).await });

        match tds_client {
            Ok(client) => Ok(PyCoreConnection {
                runtime,
                tds_client: Some(Arc::new(Mutex::new(client))),
                is_closed: false,
            }),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "Failed to connect to SQL Server: {e}"
            ))),
        }
    }

    fn close(&mut self) -> PyResult<()> {
        if !self.is_closed {
            self.tds_client = None;
            self.is_closed = true;
        }
        Ok(())
    }

    fn cursor(&self) -> PyResult<crate::cursor::PyCoreCursor> {
        if self.is_closed {
            return Err(PyRuntimeError::new_err("Connection is closed"));
        }

        if let Some(client) = &self.tds_client {
            // Pass runtime handle to cursor so it can use the same runtime
            let handle = self.runtime.handle().clone();
            Ok(crate::cursor::PyCoreCursor::new(client.clone(), handle))
        } else {
            Err(PyRuntimeError::new_err("No active connection"))
        }
    }

    fn commit(&mut self) -> PyResult<()> {
        if self.is_closed {
            return Err(PyRuntimeError::new_err("Connection is closed"));
        }

        // TODO: Implement transaction commit
        Ok(())
    }

    fn rollback(&mut self) -> PyResult<()> {
        if self.is_closed {
            return Err(PyRuntimeError::new_err("Connection is closed"));
        }

        // TODO: Implement transaction rollback
        Ok(())
    }

    fn is_connected(&self) -> PyResult<bool> {
        Ok(!self.is_closed && self.tds_client.is_some())
    }

    fn __repr__(&self) -> String {
        if self.is_closed {
            "DdbcConnection(closed)".to_string()
        } else {
            "DdbcConnection(connected)".to_string()
        }
    }
}

impl PyCoreConnection {
    /// Convert Python dict (ClientContext fields) to Rust ClientContext
    fn dict_to_client_context(dict: &Bound<'_, PyDict>) -> PyResult<ClientContext> {
        // Extract required fields with defaults
        let server = dict
            .get_item("server")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| "localhost".to_string());

        // Parse server string to get TransportContext (handles host:port, host,port, named pipes, localdb, etc.)
        let transport_context = TransportContext::parse_server_name(&server, 1433);

        let user_name = dict
            .get_item("user_name")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_default();

        let password = dict
            .get_item("password")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_default();

        let database = dict
            .get_item("database")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_default();

        let application_name = dict
            .get_item("application_name")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| "mssql-python".to_string());

        let connect_timeout = dict
            .get_item("connect_timeout")?
            .and_then(|v| v.extract::<u32>().ok())
            .unwrap_or(15);

        let packet_size = dict
            .get_item("packet_size")?
            .and_then(|v| v.extract::<i16>().ok())
            .unwrap_or(4096);

        let mars_enabled = dict
            .get_item("mars_enabled")?
            .and_then(|v| v.extract::<bool>().ok())
            .unwrap_or(false);

        let trust_server_certificate = dict
            .get_item("trust_server_certificate")?
            .and_then(|v| v.extract::<bool>().ok())
            .unwrap_or(false);

        // HostnameInCertificate - used to specify the expected hostname in the server certificate
        // when it differs from the server name in the connection string
        let host_name_in_cert = dict
            .get_item("host_name_in_certificate")?
            .and_then(|v| v.extract::<String>().ok());

        // Parse encryption setting (case-insensitive)
        let encryption_str = dict
            .get_item("encryption")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| "Optional".to_string());

        let encryption_mode = match encryption_str.to_ascii_lowercase().as_str() {
            "mandatory" | "required" => EncryptionSetting::Required,
            "disabled" => EncryptionSetting::PreferOff,
            "strict" => EncryptionSetting::Strict,
            _ => EncryptionSetting::On, // Default to On (encryption after prelogin)
        };

        // ServerCertificate - path to the server certificate file for validation
        let server_certificate = dict
            .get_item("server_certificate")?
            .and_then(|v| v.extract::<String>().ok());

        let encryption_options = EncryptionOptions {
            mode: encryption_mode,
            trust_server_certificate,
            host_name_in_cert,
            server_certificate,
        };

        // Parse application intent (case-insensitive)
        let application_intent_str = dict
            .get_item("application_intent")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| "ReadWrite".to_string());

        let application_intent = match application_intent_str.to_ascii_lowercase().as_str() {
            "readonly" => ApplicationIntent::ReadOnly,
            _ => ApplicationIntent::ReadWrite,
        };

        let workstation_id = dict
            .get_item("workstation_id")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| {
                hostname::get()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            });

        let language = dict
            .get_item("language")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| "us_english".to_string());

        // IpAddressPreference - controls IPv4 vs IPv6 preference for DNS resolution
        // Values: "IPv4First", "IPv6First", "UsePlatformDefault" (default)
        // Case-insensitive comparison
        let ipaddress_preference_str = dict
            .get_item("ip_address_preference")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| "UsePlatformDefault".to_string());

        let ipaddress_preference = match ipaddress_preference_str.to_ascii_lowercase().as_str() {
            "ipv4first" => IPAddressPreference::IPv4First,
            "ipv6first" => IPAddressPreference::IPv6First,
            _ => IPAddressPreference::UsePlatformDefault,
        };

        // TCP Keep-alive settings (milliseconds)
        // Defaults: 30000ms (30s) for keep_alive, 1000ms (1s) for interval per SQL Server defaults
        // Named to match ODBC Driver's "KeepAlive" and "KeepAliveInterval" parameters
        let keep_alive_in_ms = dict
            .get_item("keep_alive")?
            .and_then(|v| v.extract::<u32>().ok())
            .unwrap_or(30_000);

        let keep_alive_interval_in_ms = dict
            .get_item("keep_alive_interval")?
            .and_then(|v| v.extract::<u32>().ok())
            .unwrap_or(1_000);

        // Extract access token (if provided, use AccessToken authentication)
        let access_token = dict
            .get_item("access_token")?
            .and_then(|v| v.extract::<String>().ok());

        // Determine authentication method based on provided credentials:
        // - access_token provided → AccessToken authentication
        // - user_name AND password provided → SQL Password authentication
        // - both access_token and credentials → Error
        // - partial credentials (only user_name or only password) → Error
        // - neither provided → Error
        let has_access_token = access_token.is_some();
        let has_user_name = !user_name.is_empty();
        let has_password = !password.is_empty();

        let authentication_method = match (has_access_token, has_user_name, has_password) {
            // Access token with any credentials → Error
            (true, true, _) | (true, _, true) => {
                return Err(PyRuntimeError::new_err(
                    "Cannot use both 'access_token' and 'user_name'/'password'. \
                     Please provide either an access token OR username/password credentials, not both.",
                ));
            }
            // Access token only → AccessToken auth
            (true, false, false) => TdsAuthenticationMethod::AccessToken,
            // Both username and password → SQL Password auth
            (false, true, true) => TdsAuthenticationMethod::Password,
            // Only username, no password → Error
            (false, true, false) => {
                return Err(PyRuntimeError::new_err(
                    "Incomplete credentials: 'user_name' provided without 'password'. \
                     Please provide both 'user_name' and 'password' for SQL authentication.",
                ));
            }
            // Only password, no username → Error
            (false, false, true) => {
                return Err(PyRuntimeError::new_err(
                    "Incomplete credentials: 'password' provided without 'user_name'. \
                     Please provide both 'user_name' and 'password' for SQL authentication.",
                ));
            }
            // Nothing provided → Error
            (false, false, false) => {
                return Err(PyRuntimeError::new_err(
                    "No authentication credentials provided. \
                     Please provide either 'access_token' or both 'user_name' and 'password'.",
                ));
            }
        };

        // Create ClientContext
        let mut context = ClientContext::new();
        context.transport_context = transport_context;
        context.user_name = user_name;
        context.password = password;
        context.database = database;
        context.application_name = application_name;
        context.connect_timeout = connect_timeout;
        context.packet_size = packet_size;
        context.mars_enabled = mars_enabled;
        context.encryption_options = encryption_options;
        context.application_intent = application_intent;
        context.workstation_id = workstation_id;
        context.language = language;
        context.ipaddress_preference = ipaddress_preference;
        context.keep_alive_in_ms = keep_alive_in_ms;
        context.keep_alive_interval_in_ms = keep_alive_interval_in_ms;
        context.tds_authentication_method = authentication_method;
        context.access_token = access_token;

        Ok(context)
    }
}
