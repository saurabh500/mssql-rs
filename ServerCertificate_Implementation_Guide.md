# ServerCertificate Connection Keyword - Implementation Guide

## Overview

The `ServerCertificate` feature has been implemented in the Rust TDS client, providing certificate pinning capabilities that match the ODBC driver behavior. This feature allows clients to specify an expected server certificate file for exact binary matching during SSL/TLS handshake.

## Implementation Details

### Core Components

1. **EncryptionOptions Extension** (`src/core.rs`)
   - Added `server_certificate: Option<String>` field to `EncryptionOptions` struct
   - Stores the path to the DER or PEM encoded X.509 certificate file

2. **Certificate Validator Module** (`src/connection/transport/certificate_validator.rs`)
   - `load_certificate_from_file()`: Loads and parses certificates from disk
   - `is_certificate_expired()`: Validates certificate expiry using x509-parser
   - `constant_time_compare()`: Performs timing-attack resistant binary comparison
   - `validate_server_certificate()`: Main validation function coordinating all checks
   - Supports both DER and PEM certificate formats with automatic detection

3. **SSL Handler Updates** (`src/connection/transport/ssl_handler.rs`)
   - Integrated certificate validation after TLS handshake
   - Implements precedence: `ServerCertificate` > `TrustServerCertificate`
   - Enforces mutual exclusivity with `HostnameInCertificate`
   - Bypasses CA chain validation when certificate pinning is enabled

4. **Error Types** (`src/error.rs`)
   - `CertificateNotFound`: File not found at specified path
   - `InvalidCertificateFormat`: Invalid or corrupted certificate data
   - `CertificateExpired`: Server certificate has expired
   - `CertificateMismatch`: Certificates don't match
   - `CertificateFileIoError`: I/O error reading certificate file
   - `NoServerCertificate`: No certificate available from TLS handshake

### Dependencies Added

```toml
# Certificate parsing and validation
x509-parser = "0.16"
base64 = "0.22"

# Testing
tempfile = "3.13"  # dev-dependency
```

## Usage Examples

### Basic Connection with Certificate Pinning

```rust
use mssql_tds::core::{EncryptionOptions, EncryptionSetting};
use mssql_tds::connection::client_context::{ClientContext, TransportContext};

let mut context = ClientContext::new();

// Configure encryption with certificate pinning
context.encryption_options = EncryptionOptions {
    mode: EncryptionSetting::Mandatory,
    trust_server_certificate: false,
    host_name_in_cert: None,
    server_certificate: Some("/path/to/server_certificate.cer".to_string()),
};

context.transport_context = TransportContext::Tcp {
    host: "myserver.database.windows.net".to_string(),
    port: 1433,
};

// Connection will validate the server's certificate against the provided file
// let transport = create_transport(...).await?;
```

### Linux/Unix Path Example

```rust
context.encryption_options.server_certificate = 
    Some("/etc/ssl/certs/sqlserver.cer".to_string());
```

### Windows Path Example

```rust
context.encryption_options.server_certificate = 
    Some(r"C:\certs\sqlserver.cer".to_string());
```

### Relative Path Example

```rust
context.encryption_options.server_certificate = 
    Some("./certs/server_cert.cer".to_string());
```

## Certificate File Preparation

### Exporting from SQL Server (Windows)

```powershell
# Find certificate thumbprint
Get-ChildItem -Path Cert:\LocalMachine\My | Where-Object {$_.Subject -like "*SQL*"}

# Export certificate to DER format
$cert = Get-Item Cert:\LocalMachine\My\THUMBPRINT
$bytes = $cert.Export([System.Security.Cryptography.X509Certificates.X509ContentType]::Cert)
[System.IO.File]::WriteAllBytes("C:\certs\sqlserver.cer", $bytes)
```

### Exporting Using OpenSSL

```bash
# Export from PEM to DER
openssl x509 -in certificate.pem -outform der -out sqlserver.cer

# Export from server (if you have the PEM file)
openssl s_client -connect myserver.database.windows.net:1433 -showcerts \
    < /dev/null 2>/dev/null | openssl x509 -outform der -out sqlserver.cer
```

### Verifying Certificate

```bash
# View certificate details (DER format)
openssl x509 -inform der -in sqlserver.cer -text -noout

# Check expiry date
openssl x509 -inform der -in sqlserver.cer -noout -enddate
```

## Security Considerations

### File Permissions

**Linux/macOS:**
```bash
# Set restrictive permissions (owner read-only)
chmod 400 /etc/ssl/certs/sqlserver.cer
chown myuser:mygroup /etc/ssl/certs/sqlserver.cer
```

**Windows:**
```powershell
# Remove inheritance and set read-only for current user
$acl = Get-Acl "C:\certs\sqlserver.cer"
$acl.SetAccessRuleProtection($true, $false)
$rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
    $env:USERNAME, "Read", "Allow"
)
$acl.AddAccessRule($rule)
Set-Acl "C:\certs\sqlserver.cer" $acl
```

### Best Practices

1. **Certificate Storage**: Store certificates in secure locations with restricted permissions
2. **Certificate Distribution**: Use secure channels to distribute certificate files
3. **Certificate Rotation**: Plan for certificate updates when server certificates are rotated
4. **Memory Security**: Certificate data is cleared from memory after validation
5. **Timing Attacks**: Binary comparison uses constant-time algorithm to prevent timing attacks

## Behavior and Precedence

### Feature Precedence

When `ServerCertificate` is specified:
- **Overrides**: `TrustServerCertificate` (warning logged)
- **Conflicts with**: `HostnameInCertificate` (connection error)
- **Works with**: All encryption modes (Mandatory, Strict, etc.)

### Validation Behavior

| Encryption Mode | ServerCertificate Behavior |
|----------------|----------------------------|
| PreferOff | Validation performed if encryption negotiated |
| On | Validation always performed |
| Required | Validation always performed |
| Strict | Validation always performed (TLS 1.2+) |

### What Gets Validated

✅ **Validated:**
- Server certificate expiry (notAfter field)
- Exact binary match (DER-encoded data)
- Certificate file format and structure

❌ **Bypassed:**
- CA chain validation
- Hostname verification
- Certificate revocation checking

## Error Handling

### Common Errors and Solutions

#### Certificate Not Found
```rust
// Error: Certificate file not found: /path/to/cert.cer
// Solution: Verify the path is correct and file exists
if !std::path::Path::new(&cert_path).exists() {
    eprintln!("Certificate file does not exist: {}", cert_path);
}
```

#### Invalid Certificate Format
```rust
// Error: Invalid certificate format in file: /path/to/cert.cer
// Solution: Ensure file contains valid DER or PEM encoded X.509 certificate
// Verify with: openssl x509 -inform der -in cert.cer -text -noout
```

#### Certificate Expired
```rust
// Error: Server certificate has expired
// Solution: Server administrator must renew the certificate
// Check expiry: openssl x509 -inform der -in cert.cer -noout -enddate
```

#### Certificate Mismatch
```rust
// Error: Server certificate validation failed: Certificate mismatch
// Solution: 
// 1. Verify you're connecting to the correct server
// 2. Server certificate may have been renewed - update your local copy
// 3. Check for man-in-the-middle attack
```

## Testing

### Unit Tests

The implementation includes comprehensive unit tests:

```bash
# Run certificate validator tests
cargo test --package mssql-tds --lib connection::transport::certificate_validator

# Run all tests
cargo test --package mssql-tds
```

### Test Coverage

- ✅ Certificate loading (DER and PEM formats)
- ✅ Expiry validation
- ✅ Binary comparison (exact match, mismatch, different sizes)
- ✅ Constant-time comparison verification
- ✅ Error handling (file not found, invalid format, I/O errors)
- ✅ PEM to DER conversion

### Integration Testing

For integration testing with a real SQL Server:

1. Export the server's certificate
2. Place it in a test directory
3. Configure `EncryptionOptions` with the certificate path
4. Verify connection succeeds with correct certificate
5. Verify connection fails with mismatched certificate

## Logging and Diagnostics

The implementation uses the `tracing` crate for diagnostic logging:

```rust
// Enable debug logging to see certificate validation details
use tracing_subscriber;

tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .init();
```

Log messages include:
- `INFO`: Certificate loading and validation success
- `DEBUG`: Certificate format detection, size information
- `WARN`: Precedence conflicts (ServerCertificate + TrustServerCertificate)
- `ERROR`: Validation failures (via error returns)

## Compatibility

### Platform Support
- ✅ Linux
- ✅ macOS  
- ✅ Windows

### TDS Version Support
- ✅ TDS 7.4 (with encryption after prelogin)
- ✅ TDS 8.0 (with strict encryption)

### Certificate Format Support
- ✅ DER-encoded X.509 certificates (.cer, .der)
- ✅ PEM-encoded X.509 certificates (.pem, .crt) - auto-converted

## Migration from TrustServerCertificate

### Before (Insecure)
```rust
context.encryption_options = EncryptionOptions {
    mode: EncryptionSetting::Mandatory,
    trust_server_certificate: true,  // ⚠️ Accepts any certificate
    host_name_in_cert: None,
    server_certificate: None,
};
```

### After (Secure with Certificate Pinning)
```rust
context.encryption_options = EncryptionOptions {
    mode: EncryptionSetting::Mandatory,
    trust_server_certificate: false,
    host_name_in_cert: None,
    server_certificate: Some("/etc/ssl/certs/sqlserver.cer".to_string()),  // ✅ Validates specific certificate
};
```

## Future Enhancements

Potential future improvements:
- Certificate caching across connections
- Support for certificate chains
- Multiple acceptable certificates
- Integration with system certificate stores
- Automatic certificate refresh on file change

## References

- Feature Specification: `ServerCertificate_Feature_Spec.md`
- ODBC Driver Implementation: Microsoft ODBC Driver 17.10+ for SQL Server
- X.509 Certificate Standard: RFC 5280
- TDS Protocol: TDS 7.4 and 8.0 specifications
