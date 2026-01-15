# Test Certificates

This directory contains test certificates for validating the ServerCertificate feature.

## Test Certificate Files

### valid_cert.pem
A valid self-signed certificate in PEM format for testing successful certificate loading and validation.
Generated with OpenSSL for testing purposes only.

### valid_cert.der  
The same certificate in DER (binary) format for testing DER file loading.

### invalid_format.txt
An invalid file that doesn't contain a valid certificate, used to test error handling.

## Generating Test Certificates

To regenerate test certificates:

```bash
# Generate a self-signed certificate (PEM format)
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out valid_cert.pem -days 3650 -nodes \
  -subj "/C=US/ST=Test/L=Test/O=Test/CN=localhost"

# Convert PEM to DER format
openssl x509 -in valid_cert.pem -outform DER -out valid_cert.der

# Create an invalid format file
echo "This is not a certificate" > invalid_format.txt
```

**Note:** These certificates are for testing only and should never be used in production.
