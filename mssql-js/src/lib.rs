// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{
    env,
    sync::{Arc, Once},
};

use mssql_tds::{
    connection::client_context::ClientContext,
    connection_provider::tds_connection_provider::TdsConnectionProvider,
};
use tokio::sync::Mutex;
use tracing::Level;

use crate::{connection::Connection, context::JsClientContext, ffidatatypes::CollationMetadata};
use tracing_subscriber::FmtSubscriber;

#[macro_use]
extern crate napi_derive;

pub mod connection;
pub mod context;
pub mod datatypes;
pub mod ffidatatypes;

static INIT: Once = Once::new();

#[napi]
pub async fn connect(context: JsClientContext) -> napi::Result<Connection> {
    let enable_trace = env::var("MSSQLJS_TRACE")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap();

    if enable_trace {
        INIT.call_once(|| {
            // Initialize the global tracing subscriber
            let subscriber = FmtSubscriber::builder()
                .with_max_level(Level::TRACE)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("Setting default subscriber failed");
        });
    }
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
            let collation = CollationMetadata::from(client.get_collation());
            let connection = Connection {
                tds_client: Arc::new(Mutex::new(client)),
                collation: Some(collation),
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
