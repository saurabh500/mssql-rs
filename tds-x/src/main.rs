use tds_x::connection::client_context::ClientContext;
use tds_x::connection_provider::tds_connection_provider::TdsConnectionProvider;

#[tokio::main]
async fn main() {
    let context = ClientContext {
        server_name: "localhost".to_string(),
        port: 1433,
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
