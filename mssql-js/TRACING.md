# Tracing in mssql-js

This document describes how to configure and use tracing in the `mssql-js` library, including all supported environment variables and their behaviors.

## Environment Variables

### `MSSQLJS_TRACE`
- **Type:** `bool` (`true` or `false`)
- **Default:** `false`
- **Description:**
  - Enables or disables tracing.
  - If not set or set to `false`, tracing is disabled.
  - If set to `true`, tracing is enabled and the other tracing environment variables are considered.

### `MSSQLJS_TRACE_OUTPUTS`
- **Type:** `string` (comma-separated list: `file`, `console`)
- **Default:** `file`
- **Description:**
  - Specifies where trace output is sent.
  - Supported values: `file`, `console`, or both (e.g. `file,console`).
  - Unknown values are ignored with a warning.
  - If neither `file` nor `console` is specified, tracing is not enabled and a warning is printed.

### `MSSQLJS_TRACE_DIR`
- **Type:** `string` (directory path)
- **Default:** System temp directory + `/mssql-js` subfolder
- **Description:**
  - Specifies the directory where the trace log file (`mssqljs_trace.log`) will be written (if `file` output is enabled).
  - If the directory does not exist, the library will attempt to create it.
  - If not set, the log file will be placed in the system temp directory under a `mssql-js` subfolder (created if needed).
  - **Cross-platform:** Works on both Windows and Unix systems.

### `MSSQLJS_TRACE_LEVEL`
- **Type:** `string` (log level or filter expression)
- **Default:** `info`
- **Description:**
  - Sets the minimum log level for tracing output. Accepts standard levels: `error`, `warn`, `info`, `debug`, `trace`.
  - You can also use advanced filter expressions supported by [`tracing_subscriber::EnvFilter`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html).
  - If set to an invalid value, a warning is printed to the console and the log level falls back to `info`.

## Behaviors

- **Trace Outputs:**
  - Controlled by `MSSQLJS_TRACE_OUTPUTS`.
  - `file`: Write logs to a file (see `MSSQLJS_TRACE_DIR`).
  - `console`: Write logs to the console (stdout).
  - Both can be enabled simultaneously.
  - If neither is enabled, tracing is not set up and a warning is printed.

- **Trace File Location:**
  - If `MSSQLJS_TRACE_DIR` is set, the log file is written to that directory as `mssqljs_trace.log`.
  - If not set, the log file is written to the system temp directory under a `mssql-js` subfolder.

- **Directory Creation:**
  - If the target directory does not exist, the library attempts to create it.

- **File Creation Failure:**
  - If the log file cannot be created (e.g., due to permissions), a warning is printed to the console and file logging is disabled. Console logging remains enabled if requested.

- **Invalid Trace Level:**
  - If `MSSQLJS_TRACE_LEVEL` is set to an invalid value, a warning is printed to the console and the log level falls back to `info`.

- **Unknown Trace Output:**
  - If `MSSQLJS_TRACE_OUTPUTS` contains an unknown value, a warning is printed and that value is ignored.

- **Thread Safety:**
  - Tracing is initialized only once, even if `connect` is called multiple times.

## Example Usage

```sh
# Enable tracing, log to file only (default)
export MSSQLJS_TRACE=true

# Console only (Kubernetes)
export MSSQLJS_TRACE=true
export MSSQLJS_TRACE_OUTPUTS=console

# Both file and console
export MSSQLJS_TRACE=true
export MSSQLJS_TRACE_OUTPUTS=file,console

# Custom log directory
export MSSQLJS_TRACE=true
export MSSQLJS_TRACE_OUTPUTS=file
export MSSQLJS_TRACE_DIR=/var/log/mssqljs

# Set log level to debug
export MSSQLJS_TRACE=true
export MSSQLJS_TRACE_LEVEL=debug

# On Windows, you might use:
set MSSQLJS_TRACE=true
set MSSQLJS_TRACE_OUTPUTS=console,file
set MSSQLJS_TRACE_DIR=C:\\logs\\mssqljs
set MSSQLJS_TRACE_LEVEL=trace
```

## Troubleshooting

- **No log file is created:**
  - Check that `MSSQLJS_TRACE` is set to `true`.
  - Ensure `MSSQLJS_TRACE_OUTPUTS` includes `file`.
  - Ensure the directory specified by `MSSQLJS_TRACE_DIR` exists and is writable, or that the system temp directory is writable.
  - Check for warnings in the console about file creation failures.

- **No logs at expected level:**
  - Check the value of `MSSQLJS_TRACE_LEVEL`.
  - If an invalid value is set, a warning will be printed and the log level will default to `info`.

- **No logs at all:**
  - Ensure at least one valid output (`file` or `console`) is specified in `MSSQLJS_TRACE_OUTPUTS`.
  - Check for warnings about unknown outputs.

## References
- [tracing crate documentation](https://docs.rs/tracing)
- [tracing-subscriber EnvFilter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html)
