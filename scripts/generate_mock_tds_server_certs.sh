#!/bin/bash
# Generate test certificates for mock TDS server TLS tests
# This script generates self-signed certificates for testing purposes only.
# Do NOT use these certificates in production.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="$SCRIPT_DIR/../mssql-tds/tests/test_certificates"

# Create directory if it doesn't exist
mkdir -p "$CERT_DIR"

echo "Generating test certificates for mock TDS server tests..."

# Generate self-signed certificate and private key
openssl req -x509 -newkey rsa:2048 \
    -keyout "$CERT_DIR/key.pem" \
    -out "$CERT_DIR/valid_cert.pem" \
    -days 3650 \
    -nodes \
    -subj "/C=US/ST=Test/L=Test/O=Test/CN=localhost" 2>/dev/null

# Convert to DER format
openssl x509 -in "$CERT_DIR/valid_cert.pem" -outform DER -out "$CERT_DIR/valid_cert.der" 2>/dev/null

echo "Test certificates generated in $CERT_DIR:"
echo "  - key.pem (private key)"
echo "  - valid_cert.pem (certificate in PEM format)"
echo "  - valid_cert.der (certificate in DER format)"
echo ""
echo "Note: These are for testing only. Do not commit key.pem or valid_cert.pem to git."
