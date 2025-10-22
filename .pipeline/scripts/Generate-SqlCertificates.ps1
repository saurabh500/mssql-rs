# Generate and Install SSL Certificates for SQL Server
# This script creates self-signed certificates for SQL Server TLS encryption

param (
    [string]$InstanceName = "MSSQLSERVER"
)

Write-Host "=== SQL Server Certificate Generation ===" -ForegroundColor Cyan
Write-Host "Instance: $InstanceName" -ForegroundColor Yellow

function Copy-To-Root-Store($cert) {
    Write-Host "Installing certificate to trusted root store..." -ForegroundColor Yellow
    try {
        $certPath = "MyRootCA.cer"
        Export-Certificate -Cert $cert -FilePath $certPath -Type CERT | Out-Null
        Import-Certificate -FilePath $certPath -CertStoreLocation "Cert:\LocalMachine\Root" | Out-Null
        Write-Host "✅ Certificate installed to trusted root store" -ForegroundColor Green
        
        # Clean up temporary file
        Remove-Item -Path $certPath -Force -ErrorAction SilentlyContinue
    } catch {
        Write-Error "Failed to install certificate to root store: $($_.Exception.Message)"
    }
}

function New-And-Install-Certificates($instanceName) {
    Write-Output "Instance name received is " + $instanceName
    $certStorePath  = "Cert:\LocalMachine\My"
    $sqlServiceAccount = (Get-WmiObject -Class Win32_Service | Where-Object { $_.Name -like "*$instanceName" }).StartName
    Write-Output $sqlServiceAccount

    $cert = New-SelfSignedCertificate -Type SSLServerAuthentication -Subject "CN=$env:COMPUTERNAME" -FriendlyName "SQL Server Test self-signed" -DnsName "$env:COMPUTERNAME",'localhost'  -KeyAlgorithm RSA -KeyLength 2048 -Hash 'SHA256' -TextExtension '2.5.29.37={text}1.3.6.1.5.5.7.3.1' -NotAfter (Get-Date).AddMonths(24) -KeySpec KeyExchange -Provider "Microsoft RSA SChannel Cryptographic Provider" -CertStoreLocation $certStorePath

    $thumbprint = $cert.Thumbprint

    $certificate = Get-ChildItem $certStorePath | Where-Object thumbprint -eq $thumbprint

    $rsaCert = [System.Security.Cryptography.X509Certificates.RSACertificateExtensions]::GetRSAPrivateKey($certificate)

    $fileName = $rsaCert.key.UniqueName

    $path = "$env:ALLUSERSPROFILE\Microsoft\Crypto\RSA\MachineKeys\$fileName"

    $permissions = Get-Acl -Path $path

    $access_rule = New-Object System.Security.AccessControl.FileSystemAccessRule($sqlServiceAccount, 'Read', 'None', 'None', 'Allow')

    $permissions.AddAccessRule($access_rule)

    Set-Acl -Path $path -AclObject $permissions

    $registryPath = "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\MSSQL17.$instanceName\MSSQLServer\SuperSocketNetLib"

    Set-ItemProperty -Path $registryPath -Name "Certificate" -Value $thumbprint

    Restart-Service -Name "MSSQLSERVER"
    Copy-To-Root-Store -cert $cert
}

Get-WmiObject -Class Win32_Service 

(Get-WmiObject -Class Win32_Service | Where-Object { $_.Name -like "*MSSQLSERVER" }).StartName

New-And-Install-Certificates -instanceName $InstanceName
