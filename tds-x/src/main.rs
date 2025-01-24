use tds_x::connection::client_context::ClientContext;
use tds_x::connection_provider::tds_connection_provider::TdsConnectionProvider;
use tds_x::core::EncryptionSetting;

#[tokio::main]
async fn main() {
    let context = ClientContext {
        server_name: "saurabhsingh.database.windows.net".to_string(),
        port: 1433,
        encryption: EncryptionSetting::Strict,
        database: "master".to_string(),
        database_instance: "drivers".to_string(),
        ..Default::default()
    };
    let provider = TdsConnectionProvider {};
    let connection_result = provider.create_connection(&context).await;
    match connection_result {
        Ok(_connection) => {
            println!("Successfully connected");
        }
        Err(error) => {
            println!("Error: {:?}", error.to_string())
        }
    }
}
