use connect_sync_lib::connection::Connection;
use connect_sync_lib::Result;
use std::str::FromStr;
use tracing::{event, Level};
use tracing_subscriber::FmtSubscriber;

fn main() -> Result<()> {
    let trace_level = Level::from_str(
        std::env::var("CONNECT_TRACE_LEVEL")
            .unwrap_or("DEBUG".to_string())
            .as_str(),
    )
    .unwrap_or(Level::DEBUG);
    let subscriber = FmtSubscriber::builder()
        .with_max_level(trace_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");

    let host = std::env::args()
        .nth(3)
        .unwrap_or(String::from("localhost:1433"));
    let user = std::env::args().nth(2).unwrap_or(String::from("sa"));
    let password = std::env::args().nth(1)
        .unwrap_or_else(|| std::env::var("CONNECT_PASSWORD")
        .expect("No password provided.\nYou can set the password on the command line or with the CONNECT_PASSWORD environment variable.\n\nUsage: connect-sync-app Password [User] [Host]"));

    event!(Level::INFO, "Connecting to {}.", host);
    let _connection = Connection::connect(&host, &user, &password)?;

    Ok(())
}
