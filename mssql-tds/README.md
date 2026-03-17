# mssql-tds

Async Rust implementation of the TDS (Tabular Data Stream) protocol for SQL Server
and Azure SQL Database.

## What it does

Low-level TDS client handling connection negotiation (prelogin, TLS, login7),
query execution, result set streaming, bulk copy, RPC calls, and transaction
management. Built on Tokio.

## Feature flags

| Flag | Default | Description |
|------|---------|-------------|
| `integrated-auth` | **yes** | Enables both `sspi` and `gssapi` |
| `sspi` | via `integrated-auth` | Windows SSPI (Kerberos/NTLM) |
| `gssapi` | via `integrated-auth` | Unix GSSAPI (Kerberos) via runtime `dlopen` |

Disable the default to drop platform-specific auth dependencies:

```toml
mssql-tds = { version = "0.1", default-features = false }
```

## Quick start

```rust,no_run
use mssql_tds::connection::client_context::ClientContext;
use mssql_tds::connection::tds_client::ResultSetClient;
use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
use mssql_tds::core::TdsResult;

#[tokio::main]
async fn main() -> TdsResult<()> {
    let mut context = ClientContext::default();
    context.user_name = std::env::var("DB_USER").unwrap_or("<user>".into());
    context.password = std::env::var("DB_PASSWORD").unwrap_or("<password>".into());
    context.database = "master".into();

    let provider = TdsConnectionProvider {};
    let mut client = provider
        .create_client(context, "tcp:localhost,1433", None)
        .await?;

    client
        .execute("SELECT 1 AS value".into(), None, None)
        .await?;

    if let Some(rs) = client.get_current_resultset() {
        while let Some(row) = rs.next_row().await? {
            println!("{row:?}");
        }
    }

    client.close_query().await?;
    Ok(())
}
```

## License

MIT
