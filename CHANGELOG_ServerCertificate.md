# Changelog Entry - ServerCertificate Feature

## [Unreleased]

### Added
- **ServerCertificate Connection Keyword**: Implemented certificate pinning capability matching ODBC Driver 17.10+ behavior
  - Added `server_certificate` field to `EncryptionOptions` struct for specifying certificate file path
  - Created `certificate_validator` module with comprehensive validation logic
  - Support for both DER and PEM encoded X.509 certificates with automatic format detection
  - Constant-time binary comparison to prevent timing attacks
  - Certificate expiry validation using x509-parser
  - Platform support: Windows, Linux, macOS
  - TDS version support: TDS 7.4 and TDS 8.0

### Security Enhancements
- Certificate pinning allows exact binary matching of server certificates
- Bypasses CA chain validation when ServerCertificate is specified
- Protects against man-in-the-middle attacks with compromised or rogue certificates
- Memory security: Certificate data is cleared after validation
- Timing attack protection: Constant-time binary comparison implementation

### Error Handling
- Added 6 new certificate-specific error variants:
  - `CertificateNotFound`: File not found at specified path
  - `InvalidCertificateFormat`: Invalid or corrupted certificate data
  - `CertificateExpired`: Server certificate has expired
  - `CertificateMismatch`: Certificates don't match
  - `CertificateFileIoError`: I/O error reading certificate file
  - `NoServerCertificate`: No certificate available from TLS handshake
- All errors include helpful suggestions for resolution

### Dependencies
- Added `x509-parser = "0.16"` for certificate parsing and validation
- Added `base64 = "0.22"` for PEM format support
- Added `tempfile = "3.13"` as dev-dependency for testing

### Documentation
- Created `ServerCertificate_Implementation_Guide.md` with comprehensive usage examples
- Created `ServerCertificate_Implementation_Summary.md` documenting all changes
- Added example code in `examples/server_certificate_usage.rs`
- Extensive inline documentation in code

### Behavior
- `ServerCertificate` takes precedence over `TrustServerCertificate` (warning logged if both set)
- `ServerCertificate` is mutually exclusive with `HostnameInCertificate` (error if both set)
- Works with all encryption modes: PreferOff, On, Required, Strict
- CA chain validation and hostname verification are bypassed when certificate pinning is enabled

### Testing
- Added 10 comprehensive unit tests for certificate validation
- All 567 existing tests continue to pass
- Test coverage includes:
  - Certificate loading (DER and PEM formats)
  - Format detection and conversion
  - Expiry validation
  - Binary comparison (exact match, mismatch, different sizes)
  - Constant-time comparison verification
  - Error handling (file not found, invalid format, I/O errors)

### Migration Guide
Users currently using `TrustServerCertificate=true` (insecure) can migrate to `ServerCertificate` for enhanced security:

**Before (Insecure):**
```rust
encryption_options.trust_server_certificate = true; // Accepts ANY certificate
```

**After (Secure):**
```rust
encryption_options.server_certificate = Some("/path/to/server.cer".to_string()); // Only accepts THIS certificate
```

### Breaking Changes
None. This is a purely additive feature with no breaking changes to existing APIs.

### Known Limitations
- Certificate caching across connections not implemented (planned for future)
- Certificate chains not supported (single certificate only)
- Multiple acceptable certificates not supported
- Automatic certificate refresh on file change not implemented

### References
- Feature Specification: Based on ODBC Driver 17.10+ for SQL Server
- X.509 Standard: RFC 5280
- TDS Protocol: Versions 7.4 and 8.0
