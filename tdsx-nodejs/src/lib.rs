use std::sync::Arc;

use tds_x::{
  connection::client_context::ClientContext,
  connection_provider::tds_connection_provider::TdsConnectionProvider,
};
use tokio::sync::Mutex;

use crate::{
  connection::{Connection, TdsClient},
  context::JsClientContext,
};

#[macro_use]
extern crate napi_derive;

pub mod connection;
pub mod context;

#[napi]
pub async fn connect(context: JsClientContext) -> napi::Result<Connection> {
  let client_context: ClientContext = context.clone().into();
  let provider = TdsConnectionProvider {};
  let connection = provider
    .create_connection(client_context.clone(), None)
    .await
    .unwrap();

  let client = TdsClient::new(Arc::new(Mutex::new(connection)));

  let connection = Connection {
    tds_client: Arc::new(Mutex::new(client)),
  };
  // Here you can use the connection object as needed
  // For example, you can execute queries or perform other operations
  Ok(connection)
}
