# Generate test certificates for mock TDS server TLS tests
# This script generates self-signed certificates for testing purposes only.
# Do NOT use these certificates in production.

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$CertDir = Join-Path $ScriptDir "..\mssql-tds\tests\test_certificates"

# Create directory if it doesn't exist
if (-not (Test-Path $CertDir)) {
    New-Item -ItemType Directory -Path $CertDir -Force | Out-Null
}

Write-Host "Generating test certificates for mock TDS server tests..."

$KeyPath = Join-Path $CertDir "key.pem"
$CertPemPath = Join-Path $CertDir "valid_cert.pem"
$CertDerPath = Join-Path $CertDir "valid_cert.der"

# Generate self-signed certificate and private key using openssl
$opensslArgs = @(
    "req", "-x509", "-newkey", "rsa:2048",
    "-keyout", $KeyPath,
    "-out", $CertPemPath,
    "-days", "3650",
    "-nodes",
    "-subj", "/C=US/ST=Test/L=Test/O=Test/CN=localhost"
)

& openssl @opensslArgs 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to generate certificate. Make sure openssl is installed and in PATH."
    exit 1
}

# Convert to DER format
& openssl x509 -in $CertPemPath -outform DER -out $CertDerPath 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to convert certificate to DER format."
    exit 1
}

Write-Host "Test certificates generated in $CertDir`:"
Write-Host "  - key.pem (private key)"
Write-Host "  - valid_cert.pem (certificate in PEM format)"
Write-Host "  - valid_cert.der (certificate in DER format)"
Write-Host ""
Write-Host "Note: These are for testing only. Do not commit key.pem or valid_cert.pem to git."
