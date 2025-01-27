use tds_x::connection::client_context::ClientContext;
use tds_x::connection_provider::tds_connection_provider::TdsConnectionProvider;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let context = ClientContext {
        server_name: "saurabhsingh.database.windows.net".to_string(),
        port: 1433,
        user_name: "saurabh".to_string(),
        password: std::fs::read_to_string("/tmp/password")
            .expect("Failed to read password file")
            .trim()
            .to_string(),
        ..Default::default()
    };
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");

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
