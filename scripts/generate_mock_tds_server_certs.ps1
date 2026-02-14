# Generate test certificates for mock TDS server TLS tests
# This script generates self-signed certificates for testing purposes only.
# Do NOT use these certificates in production.
#
# This script uses native .NET/PowerShell APIs and does NOT require OpenSSL.

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
$PfxPath = Join-Path $CertDir "identity.pfx"

# Generate self-signed certificate using .NET APIs (no OpenSSL required)
try {
    # Create RSA key pair
    $rsa = [System.Security.Cryptography.RSA]::Create(2048)
    
    # Create certificate request
    $certRequest = [System.Security.Cryptography.X509Certificates.CertificateRequest]::new(
        "CN=localhost, O=Test, L=Test, ST=Test, C=US",
        $rsa,
        [System.Security.Cryptography.HashAlgorithmName]::SHA256,
        [System.Security.Cryptography.RSASignaturePadding]::Pkcs1
    )
    
    # Add Subject Alternative Name extension for localhost
    $sanBuilder = [System.Security.Cryptography.X509Certificates.SubjectAlternativeNameBuilder]::new()
    $sanBuilder.AddDnsName("localhost")
    $sanBuilder.AddIpAddress([System.Net.IPAddress]::Parse("127.0.0.1"))
    $certRequest.CertificateExtensions.Add($sanBuilder.Build())
    
    # Add Basic Constraints (not a CA)
    $certRequest.CertificateExtensions.Add(
        [System.Security.Cryptography.X509Certificates.X509BasicConstraintsExtension]::new($false, $false, 0, $true)
    )
    
    # Add Key Usage
    $certRequest.CertificateExtensions.Add(
        [System.Security.Cryptography.X509Certificates.X509KeyUsageExtension]::new(
            [System.Security.Cryptography.X509Certificates.X509KeyUsageFlags]::DigitalSignature -bor
            [System.Security.Cryptography.X509Certificates.X509KeyUsageFlags]::KeyEncipherment,
            $true
        )
    )
    
    # Add Enhanced Key Usage (Server Authentication)
    $serverAuthOid = [System.Security.Cryptography.Oid]::new("1.3.6.1.5.5.7.3.1", "Server Authentication")
    $oidCollection = [System.Security.Cryptography.OidCollection]::new()
    $oidCollection.Add($serverAuthOid) | Out-Null
    $enhancedKeyUsage = [System.Security.Cryptography.X509Certificates.X509EnhancedKeyUsageExtension]::new(
        $oidCollection,
        $true
    )
    $certRequest.CertificateExtensions.Add($enhancedKeyUsage)
    
    # Create self-signed certificate (valid for 10 years)
    $notBefore = [System.DateTimeOffset]::UtcNow
    $notAfter = $notBefore.AddYears(10)
    $cert = $certRequest.CreateSelfSigned($notBefore, $notAfter)
    
    # Export certificate in DER format
    $derBytes = $cert.RawData
    [System.IO.File]::WriteAllBytes($CertDerPath, $derBytes)
    
    # Export certificate in PEM format
    $certPem = "-----BEGIN CERTIFICATE-----`n"
    $certPem += [System.Convert]::ToBase64String($derBytes, [System.Base64FormattingOptions]::InsertLineBreaks)
    $certPem += "`n-----END CERTIFICATE-----`n"
    [System.IO.File]::WriteAllText($CertPemPath, $certPem)
    
    # Export private key in PEM format
    $keyBytes = $rsa.ExportRSAPrivateKey()
    $keyPem = "-----BEGIN RSA PRIVATE KEY-----`n"
    $keyPem += [System.Convert]::ToBase64String($keyBytes, [System.Base64FormattingOptions]::InsertLineBreaks)
    $keyPem += "`n-----END RSA PRIVATE KEY-----`n"
    [System.IO.File]::WriteAllText($KeyPath, $keyPem)
    
    # Export PKCS#12 (.pfx) file with empty password
    # This allows native-tls to load the identity without needing OpenSSL
    $pfxBytes = $cert.Export([System.Security.Cryptography.X509Certificates.X509ContentType]::Pfx, "")
    [System.IO.File]::WriteAllBytes($PfxPath, $pfxBytes)
    
    # Clean up
    $rsa.Dispose()
    $cert.Dispose()
}
catch {
    Write-Error "Failed to generate certificate: $_"
    exit 1
}

Write-Host "Test certificates generated in $CertDir`:"
Write-Host "  - key.pem (private key)"
Write-Host "  - valid_cert.pem (certificate in PEM format)"
Write-Host "  - valid_cert.der (certificate in DER format)"
Write-Host "  - identity.pfx (PKCS#12 identity, empty password)"
Write-Host ""
Write-Host "Note: These are for testing only. Do not commit key.pem or valid_cert.pem to git."
