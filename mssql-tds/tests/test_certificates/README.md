# Test Certificates

This directory contains test certificates for validating the TLS features in mock TDS server tests.

## Test Certificate Files

### valid_cert.pem / key.pem
A valid self-signed certificate and private key in PEM format for testing TLS connections.
These files are NOT tracked in git (they contain secrets). Generate them locally using the scripts below.

### valid_cert.der  
The same certificate in DER (binary) format for testing DER file loading.

### invalid_format.txt
An invalid file that doesn't contain a valid certificate, used to test error handling.

## Generating Test Certificates

Before running TLS tests, generate the test certificates locally.

### From repository root (recommended for CI/CD):

**Linux/macOS:**
```bash
./scripts/generate_mock_tds_server_certs.sh
```

**Windows (PowerShell):**
```powershell
.\scripts\generate_mock_tds_server_certs.ps1
```

### From this directory:

```bash
./generate_certs.sh
```

**Note:** These certificates are for testing only and should never be used in production.
The key.pem and valid_cert.pem files are gitignored to prevent pushing secrets.
