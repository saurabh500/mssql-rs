# Transport

This project delivers a simple Rust prototype that can connect to SQL Server:
- Windows platform
- Synchronous runtime
- TCP stream
- SQL authentication

It also demonstrates how to apply chain of responsibility pattern for creating a TDS connection.
The prototype is a foundation for developing more features later on.

## Execute SQL query
The prototype can execute a simple SQL query and parse the result set.
It is not a extensive implementation, but it is a proof of concept that we can run and respond to SQL Server.
At this stage, the prototype can only handle SQL results with VarChar SQL type.

## How to run
The prototype is a console application that requires three parameters to connect to SQL Server.
Only SQL authentication is supported at this stage.
Usage:
```Cmd
transport-app.exe [Password] [User] [Host]
```
The parameters are:
- Password: SQL Server password.
- User: SQL Server user, default is sa.
- Host: SQL Server host including port number, default is localhost:1433.

All parameters can be set in the environment variables:
- CONNECT_PASSWORD - SQL Server password.
- CONNECT_USER - SQL Server user.
- CONNECT_HOST - SQL Server host including port number.
