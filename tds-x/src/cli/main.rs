use std::io::Error;

use clap::Parser;
use futures::StreamExt;
use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::Helper;
use rustyline::Result as RustylineResult;
use rustyline::{error::ReadlineError, CompletionType, Config, Editor};

use crate::connection::tds_connection::TdsConnection;
use crate::core::TdsResult;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config_file_path: String,
}

/// Define commands for auto-completion
const COMMANDS: &[&str] = &[
    "database connect",
    "query execute",
    "transaction begin",
    "transaction end",
    "query result iterate",
    "query result metadata get",
];

/// Custom Helper for Rustyline
struct MyHelper {}

impl Helper for MyHelper {}
impl Completer for MyHelper {
    type Candidate = Pair;
    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> RustylineResult<(usize, Vec<Pair>)> {
        let mut matches = Vec::new();
        let start = line[..pos].rfind(' ').map_or(0, |pos| pos + 1);

        for &cmd in COMMANDS {
            if cmd.starts_with(&line[start..pos]) {
                matches.push(Pair {
                    display: cmd.to_string(),
                    replacement: cmd.to_string(),
                });
            }
        }

        Ok((start, matches))
    }
}
impl Hinter for MyHelper {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &rustyline::Context<'_>) -> Option<String> {
        None // No inline hints for now
    }
}
impl Highlighter for MyHelper {}
impl Validator for MyHelper {
    fn validate(&self, _ctx: &mut ValidationContext) -> RustylineResult<ValidationResult> {
        Ok(ValidationResult::Valid(None))
    }
}

struct Session<'clisession> {
    connection: Option<Box<TdsConnection<'clisession>>>,
}

#[allow(clippy::derivable_impls)]
impl Default for Session<'_> {
    fn default() -> Self {
        Self { connection: None }
    }
}

impl<'session> From<Box<TdsConnection<'session>>> for Session<'session> {
    fn from(connection: Box<TdsConnection<'session>>) -> Self {
        Self {
            connection: Some(connection),
        }
    }
}

impl Session<'_> {
    pub async fn submit_sql_batch(&mut self, sql_command: String) -> TdsResult<()> {
        let connection = self.connection.as_mut().ok_or(Error::new(
            std::io::ErrorKind::NotConnected,
            "No active connection",
        ))?;

        let batch_results = connection.execute(sql_command).await?;
        let mut result_stream = batch_results.stream_results();
        while let Some(result) = result_stream.next().await {
            let result_type = result.unwrap();
            match result_type {
                crate::query::result::QueryResultType::Update(update) => {
                    println!("Rows updated {}", update);
                }
                crate::query::result::QueryResultType::ResultSet(result_set) => {
                    let mut row_stream = result_set.into_row_stream().await?;

                    while let Some(some_row) = row_stream.next().await {
                        let mut row = some_row?;
                        while let Some(cell) = row.next().await {
                            print!("{:?} | ", cell.unwrap().get_value());
                        }
                        println!();
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(feature = "cli")]
pub async fn main_cli() -> Result<(), Box<dyn std::error::Error>> {
    // let args = Args::parse();
    // println!("{:?}", args);
    // let _config_file = args.config_file_path;

    // Create the Rustyline configuration

    // let subscriber = FmtSubscriber::builder()
    //     .with_max_level(Level::DEBUG)
    //     .finish();
    // tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");
    // use tracing::Level;
    // use tracing_subscriber::FmtSubscriber;

    use crate::{
        connection::client_context::ClientContext,
        connection_provider::tds_connection_provider::TdsConnectionProvider,
    };
    let config = Config::builder()
        .completion_type(CompletionType::List) // Show list of completions
        .build();

    let mut rl = Editor::with_config(config)?;
    let helper = MyHelper {};
    rl.set_helper(Some(helper));

    let context = ClientContext {
        server_name: "saurabhsingh.database.windows.net".to_string(),
        port: 1433,
        user_name: "saurabh".to_string(),
        password: std::fs::read_to_string("/tmp/password")
            .expect("Failed to read password file")
            .trim()
            .to_string(),
        database: "drivers".to_string(),
        ..Default::default()
    };
    let provider = TdsConnectionProvider {};
    let connection_result = provider.create_connection(&context).await;
    let mut session = match connection_result {
        Ok(_connection) => {
            println!("Successfully connected");
            Ok(Session::from(Box::new(_connection)))
        }
        Err(error) => {
            println!("Error: {:?}", error.to_string());
            Err(error)
        }
    };

    println!("Enter your SQL commands. Type 'exit' to quit.");

    loop {
        // Read user input with autocompletion enabled
        let readline = rl.readline("Tds-X> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(&line)?;
                let trimmed_command = line.trim();
                if trimmed_command.eq_ignore_ascii_case("exit") {
                    println!("Exiting...");
                    break;
                }

                session
                    .as_mut()
                    .unwrap()
                    .submit_sql_batch(trimmed_command.to_string())
                    .await?;
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                println!("Exiting...");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}
