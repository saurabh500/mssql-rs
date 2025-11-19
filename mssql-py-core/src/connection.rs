use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyDict;
use tokio::runtime::Runtime;
use std::sync::Arc;
use tokio::sync::Mutex;

use mssql_tds::{
    connection::client_context::{ClientContext, TransportContext, TdsAuthenticationMethod},
    connection::tds_client::TdsClient,
    connection_provider::tds_connection_provider::TdsConnectionProvider,
    core::{EncryptionOptions, EncryptionSetting},
    message::login_options::ApplicationIntent,
};

/// Python Connection class for Core TDS backend
#[pyclass]
pub struct DdbcConnection {
    #[allow(dead_code)]  // Used for async operations in cursor execute
    runtime: Runtime,
    tds_client: Option<Arc<Mutex<TdsClient>>>,
    is_closed: bool,
}

#[pymethods]
impl DdbcConnection {
    #[new]
    fn new(client_context_dict: &PyDict) -> PyResult<Self> {
        let runtime = Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {e}")))?;
        
        // Convert PyDict to ClientContext
        let client_context = Self::dict_to_client_context(client_context_dict)?;
        
        // Connect using TdsConnectionProvider
        let provider = TdsConnectionProvider {};
        let tds_client = runtime.block_on(async {
            provider.create_client(client_context, None).await
        });
        
        match tds_client {
            Ok(client) => {
                Ok(DdbcConnection {
                    runtime,
                    tds_client: Some(Arc::new(Mutex::new(client))),
                    is_closed: false,
                })
            }
            Err(e) => {
                Err(PyRuntimeError::new_err(format!("Failed to connect to SQL Server: {e}")))
            }
        }
    }

    fn close(&mut self) -> PyResult<()> {
        if !self.is_closed {
            self.tds_client = None;
            self.is_closed = true;
        }
        Ok(())
    }

    fn cursor(&self) -> PyResult<crate::cursor::DdbcCursor> {
        if self.is_closed {
            return Err(PyRuntimeError::new_err("Connection is closed"));
        }
        
        if let Some(client) = &self.tds_client {
            Ok(crate::cursor::DdbcCursor::new(client.clone()))
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

impl DdbcConnection {
    /// Convert Python dict (ClientContext fields) to Rust ClientContext
    fn dict_to_client_context(dict: &PyDict) -> PyResult<ClientContext> {
        // Extract required fields with defaults
        let server = dict.get_item("server")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| "localhost".to_string());
        
        let port: u16 = 1433; // Default port, could be extracted from server string if it contains :port
        
        let user_name = dict.get_item("user_name")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_default();
        
        let password = dict.get_item("password")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_default();
        
        let database = dict.get_item("database")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_default();
        
        let application_name = dict.get_item("application_name")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| "mssql-python".to_string());
        
        let connect_timeout = dict.get_item("connect_timeout")?
            .and_then(|v| v.extract::<u32>().ok())
            .unwrap_or(15);
        
        let packet_size = dict.get_item("packet_size")?
            .and_then(|v| v.extract::<i16>().ok())
            .unwrap_or(4096);
        
        let mars_enabled = dict.get_item("mars_enabled")?
            .and_then(|v| v.extract::<bool>().ok())
            .unwrap_or(false);
        
        let trust_server_certificate = dict.get_item("trust_server_certificate")?
            .and_then(|v| v.extract::<bool>().ok())
            .unwrap_or(false);
        
        // Parse encryption setting
        let encryption_str = dict.get_item("encryption")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| "Optional".to_string());
        
        let encryption_mode = match encryption_str.as_str() {
            "Mandatory" | "Required" => EncryptionSetting::Required,
            "Disabled" => EncryptionSetting::PreferOff,
            "Strict" => EncryptionSetting::Strict,
            _ => EncryptionSetting::On,  // Default to On (encryption after prelogin)
        };
        
        let encryption_options = EncryptionOptions {
            mode: encryption_mode,
            trust_server_certificate,
            host_name_in_cert: None,
        };
        
        // Parse application intent
        let application_intent_str = dict.get_item("application_intent")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| "ReadWrite".to_string());
        
        let application_intent = match application_intent_str.as_str() {
            "ReadOnly" => ApplicationIntent::ReadOnly,
            _ => ApplicationIntent::ReadWrite,
        };
        
        let workstation_id = dict.get_item("workstation_id")?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_else(|| {
                hostname::get()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            });
        
        // Create ClientContext
        let mut context = ClientContext::new();
        context.transport_context = TransportContext::Tcp {
            host: server,
            port,
        };
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
        context.tds_authentication_method = TdsAuthenticationMethod::Password;
        
        Ok(context)
    }
}
