// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(not(feature = "cli"))]
fn main() {
    println!("Nothing to do here.");
}

#[cfg(feature = "cli")]
#[tokio::main]
async fn main() {
    use mssql_tds::cli;
    cli::main::main_cli().await.unwrap();
}
