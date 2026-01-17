# Test Certificates

This directory contains test certificates for validating the TLS features in mock TDS server tests.

## Test Certificate Files

### valid_cert.pem / key.pem
A valid self-signed certificate and private key in PEM format for testing TLS connections.
These files are NOT tracked in git (they contain secrets). Generate them locally using the script below.

### valid_cert.der  
The same certificate in DER (binary) format for testing DER file loading.

### invalid_format.txt
An invalid file that doesn't contain a valid certificate, used to test error handling.

## Generating Test Certificates

Before running TLS tests, generate the test certificates locally:

```bash
cd mssql-tds/tests/test_certificates

# Generate a self-signed certificate (PEM format)
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out valid_cert.pem -days 3650 -nodes \
  -subj "/C=US/ST=Test/L=Test/O=Test/CN=localhost"

# Convert PEM to DER format (optional, already tracked)
openssl x509 -in valid_cert.pem -outform DER -out valid_cert.der
```

Or use the helper script:
```bash
./generate_certs.sh
```

**Note:** These certificates are for testing only and should never be used in production.
The key.pem and valid_cert.pem files are gitignored to prevent pushing secrets.
