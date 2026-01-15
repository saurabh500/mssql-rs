// Example: Using ServerCertificate for Certificate Pinning
// This example demonstrates how to use the ServerCertificate feature
// to enforce certificate pinning when connecting to SQL Server

use mssql_tds::connection::client_context::{ClientContext, TransportContext};
use mssql_tds::core::{EncryptionOptions, EncryptionSetting};

fn main() {
    // Example 1: Basic Certificate Pinning (Linux/Unix)
    println!("Example 1: Basic Certificate Pinning");
    let mut context1 = ClientContext::new();
    
    context1.encryption_options = EncryptionOptions {
        mode: EncryptionSetting::Mandatory,
        trust_server_certificate: false,
        host_name_in_cert: None,
        // Specify the path to the server's certificate
        server_certificate: Some("/etc/ssl/certs/sqlserver.cer".to_string()),
    };
    
    context1.transport_context = TransportContext::Tcp {
        host: "myserver.database.windows.net".to_string(),
        port: 1433,
    };
    
    println!("  Server: {}", context1.transport_context.get_server_name());
    println!("  Certificate: {:?}", context1.encryption_options.server_certificate);
    println!("  Encryption Mode: {:?}", context1.encryption_options.mode);
    println!();

    // Example 2: Certificate Pinning with Strict Encryption (TLS 1.3)
    println!("Example 2: Certificate Pinning with Strict Encryption");
    let mut context2 = ClientContext::new();
    
    context2.encryption_options = EncryptionOptions {
        mode: EncryptionSetting::Strict, // Forces TLS 1.3 with TDS 8.0
        trust_server_certificate: false,
        host_name_in_cert: None,
        server_certificate: Some("/etc/ssl/certs/sqlserver.cer".to_string()),
    };
    
    context2.transport_context = TransportContext::Tcp {
        host: "secure-server.example.com".to_string(),
        port: 1433,
    };
    
    println!("  Server: {}", context2.transport_context.get_server_name());
    println!("  Certificate: {:?}", context2.encryption_options.server_certificate);
    println!("  Encryption Mode: {:?} (TLS 1.3)", context2.encryption_options.mode);
    println!();

    // Example 3: Windows Path with Certificate Pinning
    #[cfg(windows)]
    {
        println!("Example 3: Windows Certificate Pinning");
        let mut context3 = ClientContext::new();
        
        context3.encryption_options = EncryptionOptions {
            mode: EncryptionSetting::Mandatory,
            trust_server_certificate: false,
            host_name_in_cert: None,
            server_certificate: Some(r"C:\certs\sqlserver.cer".to_string()),
        };
        
        context3.transport_context = TransportContext::Tcp {
            host: "localhost".to_string(),
            port: 1433,
        };
        
        println!("  Server: {}", context3.transport_context.get_server_name());
        println!("  Certificate: {:?}", context3.encryption_options.server_certificate);
        println!();
    }

    // Example 4: Relative Path with Certificate Pinning
    println!("Example 4: Relative Path Certificate Pinning");
    let mut context4 = ClientContext::new();
    
    context4.encryption_options = EncryptionOptions {
        mode: EncryptionSetting::Required,
        trust_server_certificate: false,
        host_name_in_cert: None,
        // Relative path to certificate file
        server_certificate: Some("./certs/server_cert.cer".to_string()),
    };
    
    context4.transport_context = TransportContext::Tcp {
        host: "testserver.local".to_string(),
        port: 1433,
    };
    
    println!("  Server: {}", context4.transport_context.get_server_name());
    println!("  Certificate: {:?}", context4.encryption_options.server_certificate);
    println!();

    // Example 5: Migration from TrustServerCertificate (INSECURE) to ServerCertificate (SECURE)
    println!("Example 5: Migration - Before (INSECURE)");
    let mut context5_before = ClientContext::new();
    context5_before.encryption_options = EncryptionOptions {
        mode: EncryptionSetting::Mandatory,
        trust_server_certificate: true, // ⚠️ Accepts ANY certificate
        host_name_in_cert: None,
        server_certificate: None,
    };
    println!("  TrustServerCertificate: {} (INSECURE)", 
             context5_before.encryption_options.trust_server_certificate);
    println!("  ServerCertificate: None");
    println!();
    
    println!("Example 5: Migration - After (SECURE)");
    let mut context5_after = ClientContext::new();
    context5_after.encryption_options = EncryptionOptions {
        mode: EncryptionSetting::Mandatory,
        trust_server_certificate: false,
        host_name_in_cert: None,
        server_certificate: Some("/etc/ssl/certs/sqlserver.cer".to_string()), // ✅ Only accepts THIS certificate
    };
    println!("  TrustServerCertificate: {}", 
             context5_after.encryption_options.trust_server_certificate);
    println!("  ServerCertificate: {:?} (SECURE)", 
             context5_after.encryption_options.server_certificate);
    println!();

    // Example 6: What Happens When Both Are Set (ServerCertificate takes precedence)
    println!("Example 6: Precedence - Both ServerCertificate and TrustServerCertificate Set");
    let mut context6 = ClientContext::new();
    context6.encryption_options = EncryptionOptions {
        mode: EncryptionSetting::Mandatory,
        trust_server_certificate: true, // Will be ignored with a warning
        host_name_in_cert: None,
        server_certificate: Some("/etc/ssl/certs/sqlserver.cer".to_string()), // Takes precedence
    };
    println!("  TrustServerCertificate: {} (IGNORED - warning will be logged)", 
             context6.encryption_options.trust_server_certificate);
    println!("  ServerCertificate: {:?} (ACTIVE)", 
             context6.encryption_options.server_certificate);
    println!("  Note: A warning will be logged during connection");
    println!();

    println!("Summary:");
    println!("========");
    println!("✅ ServerCertificate enables certificate pinning");
    println!("✅ Supports DER and PEM certificate formats");
    println!("✅ Works on Windows, Linux, and macOS");
    println!("✅ Compatible with all TDS encryption modes");
    println!("✅ Provides security against MITM attacks");
    println!("⚠️  ServerCertificate takes precedence over TrustServerCertificate");
    println!("⚠️  ServerCertificate is mutually exclusive with HostnameInCertificate");
}

// Additional helper functions for production use

/// Validates that the certificate file exists and is readable
#[cfg(not(test))]
fn validate_certificate_file(path: &str) -> Result<(), String> {
    use std::path::Path;
    
    let cert_path = Path::new(path);
    
    if !cert_path.exists() {
        return Err(format!("Certificate file not found: {}", path));
    }
    
    if !cert_path.is_file() {
        return Err(format!("Certificate path is not a file: {}", path));
    }
    
    // Check if file is readable
    match std::fs::metadata(path) {
        Ok(metadata) => {
            if metadata.len() == 0 {
                return Err(format!("Certificate file is empty: {}", path));
            }
            Ok(())
        }
        Err(e) => Err(format!("Cannot access certificate file: {} - {}", path, e)),
    }
}

/// Creates an EncryptionOptions with secure defaults for certificate pinning
#[cfg(not(test))]
fn create_secure_encryption_options(cert_path: String) -> Result<EncryptionOptions, String> {
    // Validate certificate file first
    validate_certificate_file(&cert_path)?;
    
    Ok(EncryptionOptions {
        mode: EncryptionSetting::Mandatory,
        trust_server_certificate: false,
        host_name_in_cert: None,
        server_certificate: Some(cert_path),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_certificate_field_present() {
        let context = ClientContext::new();
        assert!(context.encryption_options.server_certificate.is_none());
    }

    #[test]
    fn test_server_certificate_can_be_set() {
        let mut context = ClientContext::new();
        context.encryption_options.server_certificate = 
            Some("/path/to/cert.cer".to_string());
        
        assert_eq!(
            context.encryption_options.server_certificate,
            Some("/path/to/cert.cer".to_string())
        );
    }

    #[test]
    fn test_encryption_options_with_certificate() {
        let options = EncryptionOptions {
            mode: EncryptionSetting::Mandatory,
            trust_server_certificate: false,
            host_name_in_cert: None,
            server_certificate: Some("/etc/ssl/certs/test.cer".to_string()),
        };
        
        assert_eq!(options.mode, EncryptionSetting::Mandatory);
        assert!(!options.trust_server_certificate);
        assert!(options.host_name_in_cert.is_none());
        assert!(options.server_certificate.is_some());
    }
}
