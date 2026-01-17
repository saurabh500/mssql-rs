#!/bin/bash
# Generate test certificates for TLS testing
# These certificates are for testing only - do not use in production

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Generating test certificates..."

# Generate self-signed certificate and private key
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out valid_cert.pem -days 3650 -nodes \
  -subj "/C=US/ST=Test/L=Test/O=Test/CN=localhost" 2>/dev/null

# Convert to DER format
openssl x509 -in valid_cert.pem -outform DER -out valid_cert.der 2>/dev/null

echo "Test certificates generated:"
echo "  - key.pem (private key)"
echo "  - valid_cert.pem (certificate in PEM format)"
echo "  - valid_cert.der (certificate in DER format)"
echo ""
echo "Note: These are for testing only. Do not commit key.pem or valid_cert.pem to git."
