use tracing::{event, Level};
use tracing_subscriber::FmtSubscriber;
use connect_sync_lib::connection::Connection;
use connect_sync_lib::Result;

fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder().with_max_level(Level::DEBUG).finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");

    let host = std::env::args().nth(3).unwrap_or(String::from("localhost:1433"));
    let user = std::env::args().nth(2).unwrap_or(String::from("sa"));
    let password = std::env::args().nth(1).expect("No password parameter.\n\nUsage: connect-sync-app Password [User] [Host]");

    let _connection = Connection::connect(&host, &user, &password)?;
    event!(Level::INFO, "Login acknowledged!");

    Ok(())
}

