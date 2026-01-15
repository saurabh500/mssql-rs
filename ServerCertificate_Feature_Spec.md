# ServerCertificate Connection Keyword - Feature Specification

## Document Information
- **Feature Name**: ServerCertificate Connection Keyword
- **Version**: 1.0
- **Date**: January 15, 2026
- **Status**: Draft
- **Reference Implementation**: ODBC Driver 17.10+ for SQL Server

## 1. Overview

### 1.1 Purpose
The ServerCertificate connection keyword provides a mechanism for **certificate pinning** by allowing clients to specify an expected server certificate file. The driver performs an exact binary match between the provided certificate and the certificate presented by the SQL Server during the SSL/TLS handshake.

### 1.2 Motivation
- **Enhanced Security**: Prevents man-in-the-middle attacks by ensuring connections only to servers with a specific, known certificate
- **Certificate Pinning**: Implements certificate pinning without requiring custom certificate validation callbacks
- **Compliance**: Meets regulatory requirements for explicit certificate verification
- **Trust Model**: Provides an alternative to trusting Certificate Authorities (CAs) by directly validating the server certificate

### 1.3 Scope
This feature applies to:
- SSL/TLS encrypted connections to SQL Server
- All authentication modes that support encryption
- Cross-platform implementations (Windows, Linux, macOS)
- Connections using both Mandatory and Strict encryption modes

## 2. Requirements

### 2.1 Functional Requirements

#### FR-1: Connection String Keyword
- **ID**: FR-1
- **Requirement**: The driver MUST accept a new connection string keyword `ServerCertificate`
- **Value Type**: String (file path)
- **Example**: `ServerCertificate=/path/to/server_certificate.cer`

#### FR-2: Certificate File Format
- **ID**: FR-2
- **Requirement**: The driver MUST support DER-encoded X.509 certificates
- **Optional**: MAY support PEM-encoded certificates (with automatic conversion)
- **File Extensions**: `.cer`, `.crt`, `.der`, `.pem`

#### FR-3: Certificate Retrieval
- **ID**: FR-3
- **Requirement**: The driver MUST:
  - Read the certificate file from the specified path
  - Retrieve the server's certificate during the SSL/TLS handshake
  - Handle file I/O errors gracefully with appropriate error messages

#### FR-4: Certificate Validation
- **ID**: FR-4
- **Requirement**: The driver MUST perform the following validation checks:
  1. **Expiry Check**: Verify the server certificate has not expired
  2. **Exact Binary Match**: Compare the DER-encoded certificate data byte-by-byte
  3. **Size Check**: Verify certificate sizes match before binary comparison

#### FR-5: Validation Bypass
- **ID**: FR-5
- **Requirement**: When ServerCertificate is specified, the driver MUST:
  - Bypass standard CA chain validation
  - Bypass hostname verification
  - Perform ONLY expiry check and exact match validation

#### FR-6: Error Handling
- **ID**: FR-6
- **Requirement**: The driver MUST report clear error messages for:
  - Certificate file not found
  - Invalid certificate format
  - Certificate mismatch
  - Expired certificate
  - I/O errors during certificate reading

#### FR-7: Connection Failure on Mismatch
- **ID**: FR-7
- **Requirement**: If certificates do not match, the driver MUST:
  - Terminate the connection immediately
  - Return a certificate validation error
  - NOT fall back to standard validation

### 2.2 Non-Functional Requirements

#### NFR-1: Performance
- Certificate reading and comparison MUST complete within 100ms on standard hardware
- File I/O should be performed once and cached for connection retries

#### NFR-2: Security
- Certificate file contents MUST NOT be logged or exposed in error messages
- Certificate comparison MUST be constant-time to prevent timing attacks
- Memory containing certificate data MUST be securely cleared after use

#### NFR-3: Compatibility
- MUST NOT break existing connection strings without ServerCertificate
- MUST be compatible with all existing encryption modes
- MUST work with all authentication types (SQL, Windows, Azure AD)

#### NFR-4: Cross-Platform
- MUST work consistently across Windows, Linux, and macOS
- File path handling MUST respect OS-specific path conventions
- Certificate loading MUST use platform-appropriate APIs

## 3. Technical Design

### 3.1 Connection String Syntax

```
ServerCertificate=<file_path>
```

**Parameters:**
- `file_path`: Absolute or relative path to the certificate file

**Examples:**
```
# Windows
ServerCertificate=C:\certs\sqlserver.cer

# Linux/macOS
ServerCertificate=/etc/ssl/certs/sqlserver.cer
ServerCertificate=./certs/sqlserver.cer
```

### 3.2 Certificate Validation Algorithm

```
FUNCTION ValidateServerCertificate(userCertPath, encryptionContext):
    // Step 1: Load user-provided certificate
    userCert = LoadCertificateFromFile(userCertPath)
    IF userCert IS NULL:
        RETURN ERROR_CERTIFICATE_NOT_FOUND
    
    // Step 2: Get server certificate from TLS handshake
    serverCert = GetServerCertificateFromConnection(encryptionContext)
    IF serverCert IS NULL:
        RETURN ERROR_NO_SERVER_CERTIFICATE
    
    // Step 3: Check server certificate expiry
    IF IsExpired(serverCert):
        RETURN ERROR_CERTIFICATE_EXPIRED
    
    // Step 4: Compare certificate sizes
    IF userCert.size != serverCert.size:
        RETURN ERROR_CERTIFICATE_MISMATCH
    
    // Step 5: Perform byte-by-byte comparison (constant-time)
    IF NOT ConstantTimeCompare(userCert.data, serverCert.data, userCert.size):
        RETURN ERROR_CERTIFICATE_MISMATCH
    
    // Step 6: Validation successful
    RETURN SUCCESS
```

### 3.3 Implementation Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Parse Connection String                                   │
│    - Extract ServerCertificate path                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Initiate SSL/TLS Handshake                               │
│    - Configure to skip CA validation if ServerCertificate   │
│      is provided                                             │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Server Presents Certificate                               │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Load User Certificate from File                          │
│    - Handle file I/O errors                                  │
│    - Parse certificate data                                  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Validate Certificate                                      │
│    - Check expiry                                            │
│    - Perform exact binary match                              │
└──────────────────────┬──────────────────────────────────────┘
                       │
           ┌───────────┴───────────┐
           │                       │
           ▼                       ▼
    ┌──────────┐           ┌──────────┐
    │  Match   │           │ Mismatch │
    └────┬─────┘           └────┬─────┘
         │                      │
         ▼                      ▼
  ┌─────────────┐       ┌──────────────┐
  │ Connection  │       │  Connection  │
  │  Succeeds   │       │   Fails      │
  └─────────────┘       └──────────────┘
```

### 3.4 Platform-Specific Implementation

#### Windows
```cpp
// Use CryptoAPI
PCCERT_CONTEXT pCertContext;
CertCreateCertificateContext(X509_ASN_ENCODING, certData, certSize);
BOOL isExpired = CertVerifyTimeValidity(NULL, pCertContext->pCertInfo);
```

#### Linux/macOS
```cpp
// Use OpenSSL
X509* cert = d2i_X509(NULL, &certData, certSize);
ASN1_TIME* notAfter = X509_get_notAfter(cert);
int isExpired = X509_cmp_current_time(notAfter);
```

### 3.5 Data Structures

```cpp
struct ServerCertificateValidator {
    // User-provided certificate
    uint8_t* userCertData;
    size_t userCertSize;
    
    // Server certificate
    uint8_t* serverCertData;
    size_t serverCertSize;
    
    // Configuration
    char* certificateFilePath;
    bool validationEnabled;
};
```

## 4. API Design

### 4.1 Connection String Property

**Property Name**: `ServerCertificate`

**Type**: String

**Default Value**: NULL (feature disabled)

**Validation**:
- Path must be a valid file path
- File must exist and be readable
- File must contain valid certificate data

### 4.2 Error Codes

| Error Code | Error Name | Description |
|-----------|------------|-------------|
| 0x80131500 | CERT_E_CERTIFICATE_NOT_FOUND | The certificate file specified in ServerCertificate could not be found |
| 0x80131501 | CERT_E_INVALID_CERTIFICATE | The certificate file contains invalid or corrupted data |
| 0x800B010A | CERT_E_EXPIRED | The server certificate has expired |
| 0x800B0101 | CERT_E_WRONG_USAGE | The server certificate does not match the expected certificate |
| 0x80131502 | CERT_E_FILE_IO_ERROR | An I/O error occurred while reading the certificate file |

### 4.3 Error Messages

```
Certificate file not found: {FilePath}
Suggestion: Verify the ServerCertificate path is correct and the file exists.

Invalid certificate format in file: {FilePath}
Suggestion: Ensure the file contains a valid DER or PEM encoded X.509 certificate.

Server certificate has expired
Suggestion: The server's certificate is no longer valid. Contact your administrator.

Server certificate validation failed: Certificate mismatch
Suggestion: The server presented a different certificate than expected. Verify you are connecting to the correct server.

Failed to read certificate file: {FilePath}
Error: {SystemError}
Suggestion: Check file permissions and ensure the file is not locked by another process.
```

## 5. Interaction with Other Features

### 5.1 Encryption Modes

| Encryption Mode | Behavior with ServerCertificate |
|----------------|--------------------------------|
| Optional | ServerCertificate validation performed if encryption negotiated |
| Mandatory | ServerCertificate validation always performed |
| Strict | ServerCertificate validation always performed (TLS 1.2+) |

### 5.2 TrustServerCertificate

**Precedence**: `ServerCertificate` takes precedence over `TrustServerCertificate`

**Behavior**:
- If both are specified: `ServerCertificate` is used, `TrustServerCertificate` is ignored
- Error/Warning: Issue a warning if both are present in the connection string

### 5.3 HostnameInCertificate

**Compatibility**: `ServerCertificate` and `HostnameInCertificate` are mutually exclusive

**Behavior**:
- If both are specified: Return a connection error
- Validation: Driver should validate at connection string parse time

### 5.4 Certificate Revocation Checking

**Behavior**: When `ServerCertificate` is used, certificate revocation checking is **disabled** since CA chain validation is bypassed.

## 6. Testing Requirements

### 6.1 Unit Tests

#### UT-1: Certificate Loading
- Test loading valid DER certificate
- Test loading valid PEM certificate (if supported)
- Test handling of non-existent file
- Test handling of invalid certificate data
- Test handling of empty file
- Test handling of file permission errors

#### UT-2: Certificate Validation
- Test exact match with valid certificate
- Test mismatch with different certificate
- Test expired certificate detection
- Test size mismatch detection

#### UT-3: Binary Comparison
- Test constant-time comparison implementation
- Test comparison with certificates of different sizes
- Test comparison with certificates differing in single byte

#### UT-4: Connection String Parsing
- Test valid ServerCertificate values
- Test empty ServerCertificate value
- Test invalid path characters
- Test paths with spaces
- Test relative vs absolute paths

### 6.2 Integration Tests

#### IT-1: Successful Connection
- Connect to SQL Server with matching certificate
- Verify connection succeeds
- Verify data operations work correctly

#### IT-2: Failed Connection
- Connect with mismatched certificate
- Verify connection fails with appropriate error
- Verify connection does not fall back to standard validation

#### IT-3: Encryption Modes
- Test with Encrypt=Optional
- Test with Encrypt=Mandatory
- Test with Encrypt=Strict

#### IT-4: Cross-Platform
- Test on Windows with backslash paths
- Test on Linux with forward slash paths
- Test on macOS with forward slash paths

#### IT-5: Certificate Expiry
- Test with expired certificate
- Verify appropriate error message

### 6.3 Security Tests

#### ST-1: Timing Attacks
- Verify comparison is constant-time
- Measure timing variance with different certificate data

#### ST-2: Memory Security
- Verify certificate data is cleared from memory after use
- Test for memory leaks

#### ST-3: Error Message Security
- Verify certificate contents not exposed in errors
- Verify sensitive information not logged

### 6.4 Performance Tests

#### PT-1: Connection Overhead
- Measure connection time with ServerCertificate
- Compare to connection time without ServerCertificate
- Verify overhead is < 100ms

#### PT-2: File Caching
- Test repeated connections with same certificate
- Verify file is not re-read unnecessarily

## 7. Security Considerations

### 7.1 File Access Security
- Certificate files should be protected with appropriate file system permissions
- Application should run with minimum privileges necessary to read certificate files
- Consider warning users if certificate files have overly permissive permissions (world-readable)

### 7.2 Certificate Storage
- Certificate data in memory should be cleared immediately after use
- Use secure memory allocation APIs where available (e.g., SecureZeroMemory on Windows)

### 7.3 Timing Attacks
- Binary comparison MUST use constant-time comparison to prevent timing-based attacks
- Do not short-circuit comparison on first byte difference

### 7.4 Error Information Disclosure
- Error messages MUST NOT include certificate contents or hashes
- Logs MUST NOT contain certificate data
- Only include file paths in error messages, not certificate details

### 7.5 Threat Model

**Threats Mitigated**:
1. Man-in-the-Middle attacks with rogue certificates
2. Compromised Certificate Authorities
3. Certificate substitution attacks

**Threats NOT Mitigated**:
1. Compromised certificate private key on server
2. Physical access to certificate file
3. OS-level compromise allowing certificate file modification

## 8. Documentation Requirements

### 8.1 User Documentation
- Connection string reference documentation
- Security best practices guide
- Migration guide from TrustServerCertificate
- Troubleshooting guide

### 8.2 Sample Code

**Example 1: Basic Connection**
```csharp
var connectionString = "Server=myserver.database.windows.net;" +
                      "Database=mydb;" +
                      "User Id=myuser;" +
                      "Password=mypassword;" +
                      "Encrypt=Mandatory;" +
                      "ServerCertificate=/etc/ssl/certs/sqlserver.cer";

using (var connection = new SqlConnection(connectionString))
{
    connection.Open();
    // Use connection
}
```

**Example 2: With Error Handling**
```csharp
try
{
    connection.Open();
}
catch (SqlException ex) when (ex.Number == CERT_E_WRONG_USAGE)
{
    Console.WriteLine("Server certificate does not match expected certificate.");
    // Handle certificate mismatch
}
catch (SqlException ex) when (ex.Number == CERT_E_EXPIRED)
{
    Console.WriteLine("Server certificate has expired.");
    // Handle expired certificate
}
```

## 9. Migration and Compatibility

### 9.1 Backward Compatibility
- Feature is opt-in; existing applications without ServerCertificate continue to work unchanged
- No breaking changes to existing connection string keywords

### 9.2 Migration from TrustServerCertificate

**Before:**
```
TrustServerCertificate=true
```

**After:**
```
ServerCertificate=/path/to/server_certificate.cer
```

**Migration Steps:**
1. Export server certificate from SQL Server
2. Distribute certificate file to client machines
3. Update connection strings to use ServerCertificate
4. Test connections
5. Remove TrustServerCertificate=true

## 10. Open Questions

1. **Certificate Format Support**: Should we support PEM in addition to DER?
   - Recommendation: Yes, for better user experience. Automatically detect and convert.

2. **Certificate Caching**: Should certificate be cached across connections in the same process?
   - Recommendation: Yes, cache per file path with file modification time check.

3. **Relative Path Base**: What should relative paths be relative to?
   - Recommendation: Current working directory, same as other file paths.

4. **Multiple Certificates**: Should we support certificate chains or multiple acceptable certificates?
   - Recommendation: No, for v1. Single certificate only for simplicity.

5. **Certificate Rotation**: How to handle certificate rotation without application downtime?
   - Recommendation: Document that connection pool must be cleared after certificate update.

## 11. References

- ODBC Driver 17.10+ for SQL Server - Reference Implementation
- RFC 5280 - Internet X.509 Public Key Infrastructure Certificate
- TDS Protocol Specification - TDS 8.0 (Strict Encryption)
- Microsoft Docs: SQL Server Connection Encryption

## 12. Appendix

### A.1 Certificate Export from SQL Server

**Windows (SQL Server):**
```sql
-- Find certificate thumbprint
SELECT * FROM sys.certificates WHERE name = 'YourCertificateName';

-- Export using certutil
certutil -store my <thumbprint> <outputfile>.cer
```

**Linux (SQL Server):**
```bash
# Export from system store
openssl x509 -in /path/to/certificate.pem -outform der -out sqlserver.cer
```

### A.2 Certificate Inspection

```bash
# View certificate details (DER format)
openssl x509 -inform der -in sqlserver.cer -text -noout

# Convert PEM to DER
openssl x509 -in certificate.pem -outform der -out certificate.cer

# Convert DER to PEM
openssl x509 -inform der -in certificate.cer -outform pem -out certificate.pem
```

### A.3 File Permission Recommendations

**Linux/macOS:**
```bash
# Set certificate file permissions (owner read-only)
chmod 400 /etc/ssl/certs/sqlserver.cer

# Verify permissions
ls -l /etc/ssl/certs/sqlserver.cer
```

**Windows:**
```powershell
# Set certificate file ACL (user read-only)
$acl = Get-Acl "C:\certs\sqlserver.cer"
$acl.SetAccessRuleProtection($true, $false)
$rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
    $env:USERNAME, "Read", "Allow"
)
$acl.AddAccessRule($rule)
Set-Acl "C:\certs\sqlserver.cer" $acl
```

---

**Document History:**
- 2026-01-15: Initial draft based on ODBC driver implementation analysis
