use std::sync::Arc;

use mssql_tds::{
    connection::client_context::ClientContext,
    connection_provider::tds_connection_provider::TdsConnectionProvider,
};
use tokio::sync::Mutex;

use crate::{connection::Connection, context::JsClientContext};

#[macro_use]
extern crate napi_derive;

pub mod connection;
pub mod context;

#[napi]
pub async fn connect(context: JsClientContext) -> napi::Result<Connection> {
    let client_context: ClientContext = context.clone().into();
    let provider = TdsConnectionProvider {};
    let tds_client = provider.create_client(client_context.clone(), None).await;

    if tds_client.is_err() {
        return Err(napi::Error::from_reason(format!(
            "Failed to Connect to SQL Server: {}",
            tds_client.err().unwrap()
        )));
    }

    match tds_client {
        Ok(client) => {
            let connection = Connection {
                tds_client: Arc::new(Mutex::new(client)),
            };
            Ok(connection)
        }
        Err(e) => {
            // Handle the error
            Err(napi::Error::from_reason(format!(
                "Failed to create TdsClient: {e}"
            )))
        }
    }
}
