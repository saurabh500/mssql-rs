use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use transport_lib::{Parser,Config};

fn main() {
    let subscriber = FmtSubscriber::builder().with_max_level(Level::DEBUG).finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");

    let host = std::env::args().nth(3)
        .unwrap_or_else(|| std::env::var("CONNECT_HOST")
        .unwrap_or(String::from("localhost:1433")));
    let user = std::env::args().nth(2)
        .unwrap_or_else(|| std::env::var("CONNECT_USER")
        .unwrap_or(String::from("sa")));
    let password = std::env::args().nth(1)
        .unwrap_or_else(|| std::env::var("CONNECT_PASSWORD")
        .expect("No password provided.\nYou can set the password on the command line or with the CONNECT_PASSWORD environment variable.\n\nUsage: app Password [User] [Host]"));

    let config = Config::new(host, user, password);
    match Parser::connect(config) {
        Ok(_connection) => {
            println!("Success");
        }
        Err(err) => {
            println!("Error {}", err);
        }
    }
}
