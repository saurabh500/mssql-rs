use pyo3::prelude::*;
use tds_x::connection::client_context::ClientContext;
use tds_x::core::EncryptionSetting;

/// A Python class representing the client context for a TDS connection.
///
/// This class is used to encapsulate the client context information required
/// to establish a connection to a SQL server using the TDS protocol.
///
/// # Fields
///
/// * `server_name` - The name of the SQL server to connect to.
/// * `port` - The port number on which the SQL server is listening.
/// * `user_name` - The username for authentication with the SQL server.
/// * `password` - The password for authentication with the SQL server.
/// * `database` - The name of the database to connect to.
///
/// # Methods
///
/// * `new` - Creates a new instance of `PyClientContext` with the specified
///   server name, port, username, password, and database.
#[pyclass]
#[derive(Clone)]
pub struct PyClientContext {
    #[pyo3(get, set)]
    pub server_name: String,
    #[pyo3(get, set)]
    pub port: u16,
    #[pyo3(get, set)]
    pub user_name: String,
    #[pyo3(get, set)]
    pub password: String,
    #[pyo3(get, set)]
    pub database: String,
}

#[pymethods]
impl PyClientContext {
    #[new]
    fn new(
        server_name: String,
        port: u16,
        user_name: String,
        password: String,
        database: String,
    ) -> Self {
        PyClientContext {
            server_name,
            port,
            user_name,
            password,
            database,
        }
    }
}

impl From<PyClientContext> for ClientContext {
    fn from(py_ctx: PyClientContext) -> Self {
        ClientContext {
            server_name: py_ctx.server_name,
            port: py_ctx.port,
            user_name: py_ctx.user_name,
            password: py_ctx.password,
            database: py_ctx.database,
            encryption: EncryptionSetting::On,
            ..Default::default()
        }
    }
}
