---
mode: agent
---

Run `cargo fmt` to make sure all files are formatted else CI will fail.
Run `cargo bclippy` command to find any clippy errors and fix them.
We need to run integration tests. If no SQL server is running on port 1433 then launch it using `dev/dev-launchsql.sh`
Run `dev/devtests.sh` to validate if all tests are running. 