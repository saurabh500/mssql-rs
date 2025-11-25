---
mode: agent
---

Run `cargo fmt` to make sure all files are formatted else CI will fail.
Run `./scripts/bclippy.sh` to check clippy on both workspace and mssql-py-core (or `cargo bclippy` for workspace-only).
We need to run integration tests. If no SQL server is running on port 1433 then launch it using `dev/dev-launchsql.sh`
Run `dev/devtests.sh` to validate if all tests are running. 