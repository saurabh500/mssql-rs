# mssql-tds-cli

A command-line interface (CLI) tool for interacting with Microsoft SQL Server using the [mssql-tds](../mssql-tds) crate.

## Overview

`mssql-tds-cli` is a demonstration CLI built on top of the `mssql-tds` Rust crate, which provides a native implementation of the Tabular Data Stream (TDS) protocol. This tool allows you to connect to SQL Server instances, execute queries, and interact with databases directly from your terminal.

## Features

- Connect to Microsoft SQL Server using the TDS protocol
- Execute T-SQL queries and scripts
- View query results in the terminal
- Demonstrates the capabilities and usage of the `mssql-tds` library

## Usage

```
cargo run --bin mssql-tds-cli -- --config-file-path <PATH_TO_CONFIG>
```

Or, if installed:

```
mssql-tds-cli --config-file-path <PATH_TO_CONFIG>
```

### Options

- `--config-file-path <PATH>`: Path to a configuration file (required)

**Note:**
Currently, all connection details (host, port, user, password, database, etc.) are hardcoded in the CLI source code. The only configurable option via the command line is the path to a configuration file. You may extend the CLI to support more options as needed.

## Example

```
cargo run --bin mssql-tds-cli -- --config-file-path ./config.toml
```

## Why use this CLI?

- To test and debug the `mssql-tds` protocol implementation
- As a reference for building your own TDS-based tools in Rust
- For quick, scriptable access to SQL Server from the command line (with future enhancements)

## Project Structure

- `src/`: CLI source code
- `Cargo.toml`: Crate manifest
- Depends on: [`mssql-tds`](../mssql-tds)

## License

This project is licensed under the MIT License.
