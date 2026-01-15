// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Certificate validation module for ServerCertificate connection keyword
//!
//! This module implements certificate pinning by performing exact binary matching
//! between a user-provided certificate file and the server's certificate during
//! the SSL/TLS handshake.

use crate::core::TdsResult;
use crate::error::Error;
use std::fs;
use std::path::Path;
use tracing::{debug, info};

/// Load a certificate from a file path.
/// Supports both DER and PEM encoded X.509 certificates.
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

    // Check if it's PEM format and convert to DER if needed
    let der_data = if is_pem_format(&cert_data) {
        debug!("Certificate is in PEM format, converting to DER");
        convert_pem_to_der(&cert_data, path)?
    } else {
        debug!("Certificate is in DER format");
        cert_data
    };

    // Validate that it's a valid certificate structure
    validate_certificate_structure(&der_data, path)?;

    info!("Successfully loaded certificate from: {}", path);
    Ok(der_data)
}

/// Check if certificate data is in PEM format
fn is_pem_format(data: &[u8]) -> bool {
    // PEM format starts with "-----BEGIN CERTIFICATE-----"
    let pem_header = b"-----BEGIN CERTIFICATE-----";
    data.starts_with(pem_header)
}

/// Convert PEM-encoded certificate to DER format
fn convert_pem_to_der(pem_data: &[u8], path: &str) -> TdsResult<Vec<u8>> {
    // Convert bytes to string
    let pem_str = std::str::from_utf8(pem_data).map_err(|_| Error::InvalidCertificateFormat {
        path: path.to_string(),
    })?;

    // Remove PEM header and footer
    let pem_body = pem_str
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect::<Vec<&str>>()
        .join("");

    // Decode base64
    use base64::{Engine as _, engine::general_purpose};
    general_purpose::STANDARD
        .decode(pem_body.as_bytes())
        .map_err(|_| Error::InvalidCertificateFormat {
            path: path.to_string(),
        })
}

/// Validate that the certificate data has a valid X.509 structure
fn validate_certificate_structure(der_data: &[u8], path: &str) -> TdsResult<()> {
    // Basic validation: DER-encoded certificates start with 0x30 (SEQUENCE tag)
    // and should have reasonable size
    if der_data.is_empty() {
        return Err(Error::InvalidCertificateFormat {
            path: path.to_string(),
        });
    }

    if der_data[0] != 0x30 {
        return Err(Error::InvalidCertificateFormat {
            path: path.to_string(),
        });
    }

    // Reasonable size check: certificates are typically between 500 bytes and 10KB
    if der_data.len() < 100 || der_data.len() > 50_000 {
        debug!("Warning: Certificate size {} is unusual", der_data.len());
    }

    Ok(())
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
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_is_pem_format() {
        let pem_data = b"-----BEGIN CERTIFICATE-----\nMIID...";
        assert!(is_pem_format(pem_data));

        let der_data = b"\x30\x82\x03\x45";
        assert!(!is_pem_format(der_data));
    }

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

    #[test]
    fn test_validate_certificate_structure_empty() {
        let result = validate_certificate_structure(&[], "test.cer");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_certificate_structure_invalid_tag() {
        let invalid_data = vec![0x00, 0x01, 0x02];
        let result = validate_certificate_structure(&invalid_data, "test.cer");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_certificate_structure_valid() {
        // Create minimal valid DER structure (SEQUENCE tag)
        let valid_data = vec![0x30, 0x82, 0x01, 0x00]; // SEQUENCE with length
        let valid_data = [valid_data, vec![0; 256]].concat(); // Add some padding
        let result = validate_certificate_structure(&valid_data, "test.cer");
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_der_certificate() {
        // Create a temporary DER certificate file
        let mut temp_file = NamedTempFile::new().unwrap();
        let der_data = vec![0x30, 0x82, 0x01, 0x00]; // Minimal DER structure
        let der_data = [der_data, vec![0; 256]].concat();
        temp_file.write_all(&der_data).unwrap();

        let path = temp_file.path().to_str().unwrap();
        let result = load_certificate_from_file(path);
        assert!(result.is_ok());
    }

    #[test]
    fn test_convert_pem_to_der() {
        // Sample PEM certificate (base64 encoded minimal DER structure)
        let der_data = vec![0x30, 0x82, 0x01, 0x00];
        let der_data = [der_data, vec![0; 100]].concat();

        use base64::{Engine as _, engine::general_purpose};
        let base64_data = general_purpose::STANDARD.encode(&der_data);

        let pem_data = format!(
            "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----",
            base64_data
        );

        let result = convert_pem_to_der(pem_data.as_bytes(), "test.pem");
        assert!(result.is_ok());
        let converted = result.unwrap();
        assert_eq!(converted, der_data);
    }
}
