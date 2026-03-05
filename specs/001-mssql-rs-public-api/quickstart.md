# Quickstart: mssql-rs

## Add the dependency

```toml
[dependencies]
mssql-rs = "0.1"
tokio = { version = "1", features = ["full"] }
futures = "0.3"
```

## Connect and query

```rust
use mssql_rs::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = Client::connect(
        "Server=localhost,1433;Database=master;User Id=sa;Password=YourPassword123"
    ).await?;

    let rows = client.query_collect("SELECT 1 AS val, 'hello' AS msg").await?;
    for row in &rows {
        println!("{:?}", row);
    }

    client.close().await
}
```

## Stream rows

```rust
use futures::StreamExt;
use mssql_rs::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = Client::connect(
        "Server=localhost,1433;Database=mydb;User Id=sa;Password=P@ss"
    ).await?;

    let mut result_set = client.query("SELECT id, name FROM users").await?;

    while let Some(row) = result_set.next().await {
        let row = row?;
        let id: i64 = row.get(0)?;
        let name: String = row.get(1)?;
        println!("{id}: {name}");
    }

    Ok(())
}
```

## Parameterized query

```rust
use mssql_rs::{Client, Value, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = Client::connect("Server=localhost;Database=mydb;User Id=sa;Password=P@ss").await?;

    let rows = client.query_collect_with_params(
        "SELECT * FROM orders WHERE customer_id = @p1 AND status = @p2",
        &[("@p1", Value::Int(42)), ("@p2", Value::String("active".into()))],
    ).await?;

    println!("Found {} orders", rows.len());
    client.close().await
}
```

## Prepared statements

```rust
use mssql_rs::{Client, Value, Result};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = Client::connect("Server=localhost;Database=mydb;User Id=sa;Password=P@ss").await?;

    let mut stmt = client.prepare(
        "SELECT name FROM products WHERE category_id = @p1",
        &[("@p1", Value::Int(0))], // parameter template for type inference
    ).await?;

    for category in [1i64, 2, 3] {
        let mut rs = stmt.execute(&[Value::Int(category)]).await?;
        while let Some(row) = rs.next().await {
            let name: String = row?.get(0)?;
            println!("Category {category}: {name}");
        }
    }

    stmt.close().await?;
    client.close().await
}
```

## Transactions

```rust
use mssql_rs::{Client, IsolationLevel, Value, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = Client::connect("Server=localhost;Database=mydb;User Id=sa;Password=P@ss").await?;

    let mut txn = client.begin_transaction_with_isolation(IsolationLevel::Snapshot).await?;

    txn.query_with_params(
        "INSERT INTO accounts (id, balance) VALUES (@p1, @p2)",
        &[("@p1", Value::Int(1)), ("@p2", Value::Int(1000))],
    ).await?;

    txn.query_with_params(
        "UPDATE accounts SET balance = balance - @p1 WHERE id = @p2",
        &[("@p1", Value::Int(100)), ("@p2", Value::Int(1))],
    ).await?;

    txn.commit().await?;
    client.close().await
}
```

## Custom type extraction

```rust
use mssql_rs::{FromValue, Value, Result, Error};

struct UserId(i64);

impl FromValue for UserId {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Int(v) => Ok(UserId(v)),
            other => Err(Error::TypeConversion(
                format!("expected Int for UserId, got {:?}", other)
            )),
        }
    }
}

// Usage: let id: UserId = row.get(0)?;
```

## Multiple result sets

```rust
use futures::StreamExt;
use mssql_rs::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = Client::connect("Server=localhost;Database=mydb;User Id=sa;Password=P@ss").await?;

    let mut rs = client.query("SELECT 1; SELECT 2").await?;

    // First result set
    while let Some(row) = rs.next().await {
        println!("RS1: {:?}", row?.value(0)?);
    }

    // Advance to second result set
    if let Some(mut rs2) = rs.next_result_set().await? {
        while let Some(row) = rs2.next().await {
            println!("RS2: {:?}", row?.value(0)?);
        }
    }

    Ok(())
}
```
