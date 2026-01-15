// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Certificate validation module for ServerCertificate connection keyword
//!
//! This module implements certificate pinning by performing exact binary matching
//! between a user-provided certificate file and the server's certificate during
//! the SSL/TLS handshake.

use crate::core::TdsResult;
use crate::error::Error;
use native_tls::Certificate;
use std::fs;
use std::path::Path;
use tracing::{debug, info};

/// Load a certificate from a file path and convert to DER format.
/// Supports both DER and PEM encoded X.509 certificates.
/// Uses native-tls Certificate API for automatic format detection and conversion.
///
/// # Arguments
/// * `path` - Path to the certificate file
///
/// # Returns
/// * `Ok(Vec<u8>)` - DER-encoded certificate data
/// * `Err(Error)` - File not found, IO error, or invalid format
pub fn load_certificate_from_file(path: &str) -> TdsResult<Vec<u8>> {
    debug!("Loading certificate from file: {}", path);

    // Check if file exists
    if !Path::new(path).exists() {
        return Err(Error::CertificateNotFound {
            path: path.to_string(),
        });
    }

    // Read certificate file
    let cert_data = fs::read(path).map_err(|e| Error::CertificateFileIoError {
        path: path.to_string(),
        error: e.to_string(),
    })?;

    // Try to parse as PEM first, fall back to DER
    // native-tls handles the format detection and parsing
    let certificate = Certificate::from_pem(&cert_data)
        .or_else(|_| {
            debug!("Not PEM format, trying DER");
            Certificate::from_der(&cert_data)
        })
        .map_err(|_| Error::InvalidCertificateFormat {
            path: path.to_string(),
        })?;

    // Convert to DER format for binary comparison
    let der_data = certificate
        .to_der()
        .map_err(|_| Error::InvalidCertificateFormat {
            path: path.to_string(),
        })?;

    info!(
        "Successfully loaded certificate from: {} ({} bytes)",
        path,
        der_data.len()
    );
    Ok(der_data)
}

/// Check if a certificate has expired.
/// Uses the X.509 notAfter field to determine expiry.
///
/// # Arguments
/// * `der_data` - DER-encoded certificate data
///
/// # Returns
/// * `Ok(true)` - Certificate has expired
/// * `Ok(false)` - Certificate is still valid
/// * `Err(Error)` - Unable to parse certificate
pub fn is_certificate_expired(der_data: &[u8]) -> TdsResult<bool> {
    use x509_parser::prelude::*;

    let (_, cert) = X509Certificate::from_der(der_data).map_err(|e| {
        Error::ProtocolError(format!(
            "Failed to parse certificate for expiry check: {}",
            e
        ))
    })?;

    // Get the current time
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::ImplementationError(format!("System time error: {}", e)))?;

    // Get certificate validity period
    let not_after = cert.validity().not_after.timestamp();

    // Check if expired
    Ok(now.as_secs() as i64 > not_after)
}

/// Perform constant-time binary comparison of two byte arrays.
/// This prevents timing attacks during certificate validation.
///
/// # Arguments
/// * `a` - First byte array
/// * `b` - Second byte array
///
/// # Returns
/// * `true` if arrays are identical
/// * `false` if arrays differ in size or content
pub fn constant_time_compare(a: &[u8], b: &[u8]) -> bool {
    // First check sizes (this is not timing-sensitive)
    if a.len() != b.len() {
        return false;
    }

    // Constant-time comparison of contents
    // Use XOR and accumulation to avoid short-circuit evaluation
    let mut result = 0u8;
    for (byte_a, byte_b) in a.iter().zip(b.iter()) {
        result |= byte_a ^ byte_b;
    }

    result == 0
}

/// Validate server certificate against user-provided certificate.
/// Performs expiry check and exact binary match.
///
/// # Arguments
/// * `user_cert_path` - Path to user-provided certificate file
/// * `server_cert_der` - DER-encoded server certificate from TLS handshake
///
/// # Returns
/// * `Ok(())` - Certificates match and server cert is valid
/// * `Err(Error)` - Validation failed
pub fn validate_server_certificate(user_cert_path: &str, server_cert_der: &[u8]) -> TdsResult<()> {
    info!("Validating server certificate against: {}", user_cert_path);

    // Step 1: Load user-provided certificate
    let user_cert_der = load_certificate_from_file(user_cert_path)?;

    // Step 2: Check server certificate expiry
    if is_certificate_expired(server_cert_der)? {
        return Err(Error::CertificateExpired);
    }

    // Step 3: Perform exact binary match
    if !constant_time_compare(&user_cert_der, server_cert_der) {
        debug!(
            "Certificate mismatch: user cert size={}, server cert size={}",
            user_cert_der.len(),
            server_cert_der.len()
        );
        return Err(Error::CertificateMismatch);
    }

    info!("Server certificate validation successful");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_time_compare_equal() {
        let a = vec![1, 2, 3, 4, 5];
        let b = vec![1, 2, 3, 4, 5];
        assert!(constant_time_compare(&a, &b));
    }

    #[test]
    fn test_constant_time_compare_different() {
        let a = vec![1, 2, 3, 4, 5];
        let b = vec![1, 2, 3, 4, 6];
        assert!(!constant_time_compare(&a, &b));
    }

    #[test]
    fn test_constant_time_compare_different_sizes() {
        let a = vec![1, 2, 3];
        let b = vec![1, 2, 3, 4];
        assert!(!constant_time_compare(&a, &b));
    }

    #[test]
    fn test_load_certificate_file_not_found() {
        let result = load_certificate_from_file("/nonexistent/path/cert.cer");
        assert!(result.is_err());
        match result {
            Err(Error::CertificateNotFound { path }) => {
                assert!(path.contains("nonexistent"));
            }
            _ => panic!("Expected CertificateNotFound error"),
        }
    }
}
