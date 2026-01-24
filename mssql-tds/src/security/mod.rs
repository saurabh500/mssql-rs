// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Security module for integrated authentication support.
//!
//! This module provides abstractions and implementations for SSPI (Windows)
//! and GSSAPI (Linux/macOS) authentication with SQL Server.
//!
//! # Platform Support
//!
//! - **Windows**: SSPI with Negotiate, Kerberos, and NTLM packages
//! - **Linux/macOS**: GSSAPI with Kerberos only
//!
//! # Feature Flags
//!
//! - `sspi`: Enables Windows SSPI support
//! - `gssapi`: Enables Unix GSSAPI/Kerberos support

mod error;
pub mod mock;
mod security_context;
mod spn;

// Re-export public types
pub use error::SecurityError;
pub use security_context::{IntegratedAuthConfig, SecurityContext, SecurityPackage, SspiAuthToken};
pub use spn::{canonicalize_hostname, is_loopback_address, make_spn, make_spn_canonicalized};

// Platform-specific implementations
#[cfg(all(windows, feature = "sspi"))]
pub mod windows;

#[cfg(all(unix, feature = "gssapi"))]
pub mod unix;

// Re-export platform implementations
#[cfg(all(windows, feature = "sspi"))]
pub use windows::WindowsSspiContext;

#[cfg(all(unix, feature = "gssapi"))]
pub use unix::GssapiContext;

/// Creates a platform-appropriate security context.
///
/// On Windows with the `sspi` feature, creates a `WindowsSspiContext`.
/// On Unix with the `gssapi` feature, creates a `GssapiContext`.
///
/// # Arguments
///
/// * `config` - Configuration for integrated authentication
/// * `server` - The server hostname (FQDN preferred)
/// * `port` - The server port
///
/// # Errors
///
/// Returns `SecurityError` if the security context cannot be created.
#[cfg(all(windows, feature = "sspi"))]
pub fn create_security_context(
    config: &IntegratedAuthConfig,
    server: &str,
    port: u16,
) -> Result<Box<dyn SecurityContext>, SecurityError> {
    Ok(Box::new(windows::WindowsSspiContext::new(
        config, server, port,
    )?))
}

#[cfg(all(unix, feature = "gssapi"))]
pub fn create_security_context(
    config: &IntegratedAuthConfig,
    server: &str,
    port: u16,
) -> Result<Box<dyn SecurityContext>, SecurityError> {
    Ok(Box::new(unix::GssapiContext::new(config, server, port)?))
}

#[cfg(not(any(all(windows, feature = "sspi"), all(unix, feature = "gssapi"))))]
pub fn create_security_context(
    _config: &IntegratedAuthConfig,
    _server: &str,
    _port: u16,
) -> Result<Box<dyn SecurityContext>, SecurityError> {
    Err(SecurityError::NotSupported(
        "Integrated authentication requires the 'sspi' feature on Windows or 'gssapi' feature on Unix".to_string()
    ))
}
