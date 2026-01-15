// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use mssql_tds::{
    connection::client_context::{ClientContext, TdsAuthenticationMethod, TransportContext},
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

        // Parse encryption setting
        let encryption_str = dict
            .get_item("encryption")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| "Optional".to_string());

        let encryption_mode = match encryption_str.as_str() {
            "Mandatory" | "Required" => EncryptionSetting::Required,
            "Disabled" => EncryptionSetting::PreferOff,
            "Strict" => EncryptionSetting::Strict,
            _ => EncryptionSetting::On, // Default to On (encryption after prelogin)
        };

        let encryption_options = EncryptionOptions {
            mode: encryption_mode,
            trust_server_certificate,
            host_name_in_cert: None,
        };

        // Parse application intent
        let application_intent_str = dict
            .get_item("application_intent")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| "ReadWrite".to_string());

        let application_intent = match application_intent_str.as_str() {
            "ReadOnly" => ApplicationIntent::ReadOnly,
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
        context.keep_alive_in_ms = keep_alive_in_ms;
        context.keep_alive_interval_in_ms = keep_alive_interval_in_ms;
        context.tds_authentication_method = TdsAuthenticationMethod::Password;

        Ok(context)
    }
}
