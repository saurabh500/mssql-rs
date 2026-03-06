# mssql-rs Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-03-05

## Active Technologies
- Rust 1.90, Edition 2024 + `mssql-rs` (crate under test), `tokio` (async runtime + test-util), `dotenv` (env loading), `tracing-subscriber` (test diagnostics), `futures` (StreamExt for streaming tests) (002-mssql-rs-integration-tests)
- N/A — tests target a live SQL Server via connection string (002-mssql-rs-integration-tests)

- Rust 1.90, Edition 2024 + `mssql-tds` (protocol core), `futures` (Stream trait), `bytes` (chunk streaming), `tokio` (async runtime), `thiserror` (error derive), `tracing` (diagnostics) (001-mssql-rs-public-api)

## Project Structure

```text
src/
tests/
```

## Commands

cargo test [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] cargo clippy

## Code Style

Rust 1.90, Edition 2024: Follow standard conventions

## Recent Changes
- 002-mssql-rs-integration-tests: Added Rust 1.90, Edition 2024 + `mssql-rs` (crate under test), `tokio` (async runtime + test-util), `dotenv` (env loading), `tracing-subscriber` (test diagnostics), `futures` (StreamExt for streaming tests)

- 001-mssql-rs-public-api: Added Rust 1.90, Edition 2024 + `mssql-tds` (protocol core), `futures` (Stream trait), `bytes` (chunk streaming), `tokio` (async runtime), `thiserror` (error derive), `tracing` (diagnostics)

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
