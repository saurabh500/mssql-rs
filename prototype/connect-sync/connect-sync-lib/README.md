# connect-sync-lib project
connect-sync-lib project is a prototype for a Rust implementation of TDS protocol.
It shows how to implement some pieces of TDS protocol in Rust.
The current implementation establishes a connection to the SQL server by
sending a TDS prelogin and login packets.

There is a Rust application projects in `..\connect-sync-app` directory that uses the connect-sync-lib to connect to the SQL server.
The implementation uses synchronous I/O operations.
