// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Example demonstrating the action-based connection design
//!
//! This example shows how to use the new action chain architecture to
//! inspect and understand connection strategies before executing them.

use mssql_tds::connection::datasource_parser::ParsedDataSource;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Action-Based Connection Design Examples ===\n");

    // Example 1: Simple TCP with explicit port
    println!("Example 1: TCP with explicit port");
    println!("-----------------------------------");
    let parsed = ParsedDataSource::parse("tcp:myserver,1433", false)?;
    let chain = parsed.to_connection_actions(15000);
    println!("{}", chain.describe());
    println!();

    // Example 2: Named instance requiring SSRP
    println!("Example 2: Named instance (requires SSRP)");
    println!("------------------------------------------");
    let parsed = ParsedDataSource::parse("myserver\\SQLEXPRESS", false)?;
    let chain = parsed.to_connection_actions(15000);
    println!("{}", chain.describe());
    println!();

    // Example 3: Port without protocol prefix
    println!("Example 3: Port without protocol prefix (defaults to TCP)");
    println!("---------------------------------------------------------");
    let parsed = ParsedDataSource::parse("myserver,1433", false)?;
    let chain = parsed.to_connection_actions(15000);
    println!("{}", chain.describe());
    println!();

    // Example 4: Named pipe
    println!("Example 4: Named pipe connection");
    println!("---------------------------------");
    let parsed = ParsedDataSource::parse("np:\\\\myserver\\pipe\\sql\\query", false)?;
    let chain = parsed.to_connection_actions(15000);
    println!("{}", chain.describe());
    println!();

    // Example 5: Protocol auto-detection (waterfall)
    println!("Example 5: Auto-detect protocol (waterfall)");
    println!("--------------------------------------------");
    let parsed = ParsedDataSource::parse("myserver", false)?;
    let chain = parsed.to_connection_actions(15000);
    println!("{}", chain.describe());
    println!();

    // Example 6: Parallel connect (MultiSubnetFailover)
    println!("Example 6: Parallel connect (MultiSubnetFailover)");
    println!("--------------------------------------------------");
    let parsed = ParsedDataSource::parse("myserver,1433", true)?;
    let chain = parsed.to_connection_actions(15000);
    println!("{}", chain.describe());
    println!();

    // Example 7: Admin (DAC) connection
    println!("Example 7: Dedicated Admin Connection (DAC)");
    println!("--------------------------------------------");
    let parsed = ParsedDataSource::parse("admin:localhost", false)?;
    let chain = parsed.to_connection_actions(15000);
    println!("{}", chain.describe());
    println!();

    // Example 8: Shared Memory (Windows only)
    #[cfg(windows)]
    {
        println!("Example 8: Shared Memory (local only)");
        println!("--------------------------------------");
        let parsed = ParsedDataSource::parse("lpc:.", false)?;
        let chain = parsed.to_connection_actions(15000);
        println!("{}", chain.describe());
        println!();
    }

    // Example 9: LocalDB (Windows only)
    #[cfg(windows)]
    {
        println!("Example 9: LocalDB instance");
        println!("---------------------------");
        let parsed = ParsedDataSource::parse("(localdb)\\MSSQLLocalDB", false)?;
        let chain = parsed.to_connection_actions(15000);
        println!("{}", chain.describe());
        println!();
    }

    // Example 10: Instance with explicit port (no SSRP needed)
    println!("Example 10: Instance with explicit port (no SSRP)");
    println!("--------------------------------------------------");
    let parsed = ParsedDataSource::parse("myserver\\INST1,54321", false)?;
    let chain = parsed.to_connection_actions(15000);
    println!("{}", chain.describe());
    println!();

    println!("=== Summary ===");
    println!("The action chain design separates:");
    println!("1. Parsing - validating connection string syntax");
    println!("2. Strategy - determining what actions to take");
    println!("3. Execution - performing the actual connection");
    println!();
    println!("Benefits:");
    println!("- Clear, inspectable connection strategy");
    println!("- Easy to test without actual networking");
    println!("- Extensible for new connection scenarios");
    println!("- No complex conditional logic in consumers");

    Ok(())
}
