// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use once_cell::sync::OnceCell;
use std::fs::{OpenOptions, create_dir_all};
use std::path::PathBuf;
use std::sync::Once;
use tracing_appender::non_blocking;
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt};

static INIT: Once = Once::new();
static GUARD: OnceCell<tracing_appender::non_blocking::WorkerGuard> = OnceCell::new();
// === Environment Variable and Constant Strings ===
const ENV_TRACE: &str = "MSSQLJS_TRACE";
const ENV_TRACE_OUTPUTS: &str = "MSSQLJS_TRACE_OUTPUTS";
const ENV_TRACE_DIR: &str = "MSSQLJS_TRACE_DIR";
const ENV_TRACE_LEVEL: &str = "MSSQLJS_TRACE_LEVEL";
const DEFAULT_TRACE_OUTPUTS: &str = "file";
const DEFAULT_TRACE_LEVEL: &str = "info";
const TRACE_LOG_FILENAME: &str = "mssqljs_trace.log";
const TRACE_TEMP_SUBDIR: &str = "mssql-js";

fn get_trace_log_path() -> PathBuf {
    if let Ok(dir) = std::env::var(ENV_TRACE_DIR) {
        let path = PathBuf::from(dir);
        if !path.exists() {
            let _ = create_dir_all(&path);
        }
        return path.join(TRACE_LOG_FILENAME);
    }
    let mut temp_dir = std::env::temp_dir();
    temp_dir.push(TRACE_TEMP_SUBDIR);
    if !temp_dir.exists() {
        let _ = create_dir_all(&temp_dir);
    }
    temp_dir.push(TRACE_LOG_FILENAME);
    temp_dir
}

fn get_trace_filter() -> EnvFilter {
    let filter = std::env::var(ENV_TRACE_LEVEL).unwrap_or_else(|_| DEFAULT_TRACE_LEVEL.to_string());
    match EnvFilter::try_new(&filter) {
        Ok(f) => f,
        Err(e) => {
            eprintln!(
                "[mssql-js] WARNING: Invalid MSSQLJS_TRACE_LEVEL value '{filter}': {e}. Falling back to 'info'."
            );
            EnvFilter::new("info")
        }
    }
}

fn get_trace_outputs() -> (bool, bool) {
    // Returns (file_enabled, console_enabled)
    let outputs =
        std::env::var(ENV_TRACE_OUTPUTS).unwrap_or_else(|_| DEFAULT_TRACE_OUTPUTS.to_string());
    let mut file = false;
    let mut console = false;
    for output in outputs.split(',').map(|s| s.trim().to_lowercase()) {
        match output.as_str() {
            "file" => file = true,
            "console" => console = true,
            "" => {} // ignore empty
            other => {
                eprintln!(
                    "[mssql-js] WARNING: Unknown trace output '{other}'. Supported: file, console."
                );
            }
        }
    }
    (file, console)
}

pub fn init_tracing() {
    let enable_trace = std::env::var(ENV_TRACE)
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);

    if enable_trace {
        INIT.call_once(|| {
            let (file_enabled, console_enabled) = get_trace_outputs();
            if !file_enabled && !console_enabled {
                eprintln!("[mssql-js] WARNING: No valid trace outputs enabled. Set {ENV_TRACE_OUTPUTS} to 'file', 'console', or both.");
                return;
            }

            let mut guard = None;
            let mut file_layer = None;
            if file_enabled {
                let log_path = get_trace_log_path();
                match OpenOptions::new().create(true).append(true).open(&log_path) {
                    Ok(f) => {
                        let (file_writer, g) = non_blocking(f);
                        file_layer = Some(tracing_subscriber::fmt::layer()
                            .with_writer(file_writer)
                            .with_ansi(false));
                        guard = Some(g);
                    },
                    Err(e) => {
                        eprintln!(
                            "[mssql-js] WARNING: Could not create trace log file '{}': {}. File logging will be disabled.",
                            log_path.display(), e
                        );
                    }
                }
            }
            if let Some(g) = guard {
                let _ = GUARD.set(g);
            }

            let filter_layer = get_trace_filter();
            let registry = Registry::default().with(filter_layer);
            if file_layer.is_some() && console_enabled {
                let registry_with_file_and_console = registry
                    .with(file_layer.unwrap())
                    .with(tracing_subscriber::fmt::layer()
                        .with_writer(std::io::stdout)
                        .with_ansi(true));
                tracing::subscriber::set_global_default(registry_with_file_and_console)
                .expect("Setting default subscriber failed");
            } else if file_layer.is_some() {
                let registry_with_file = registry.with(file_layer.unwrap());
                tracing::subscriber::set_global_default(registry_with_file)
                .expect("Setting default subscriber failed");
            } else if console_enabled {
                let registry_with_console = registry.with(tracing_subscriber::fmt::layer()
                        .with_writer(std::io::stdout)
                        .with_ansi(true));
                tracing::subscriber::set_global_default(registry_with_console)
                .expect("Setting default subscriber failed");
            } else {
                let empty_registry = registry.with(tracing_subscriber::fmt::layer());
                tracing::subscriber::set_global_default(empty_registry)
                .expect("Setting default subscriber failed");
            };
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_get_trace_filter_default() {
        unsafe {
            env::remove_var(ENV_TRACE_LEVEL);
        }
        let filter = get_trace_filter();
        assert_eq!(filter.to_string(), DEFAULT_TRACE_LEVEL);
    }

    #[test]
    fn test_get_trace_filter_valid() {
        unsafe { env::set_var(ENV_TRACE_LEVEL, "debug") };
        let filter = get_trace_filter();
        assert_eq!(filter.to_string(), "debug");
        unsafe { env::remove_var(ENV_TRACE_LEVEL) };
    }

    #[test]
    fn test_get_trace_filter_invalid() {
        unsafe { env::set_var(ENV_TRACE_LEVEL, "banana") };
        let filter = get_trace_filter();
        assert_eq!(filter.to_string(), "banana=trace");
        unsafe { env::remove_var(ENV_TRACE_LEVEL) };
    }
}
