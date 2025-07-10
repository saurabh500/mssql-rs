param(
    [string]$Version = "20.19.3",
    [string]$InstallLocation = $null,
    [bool]$UseARM = $false
)

# Download and install the Node.js version specified
try {
    $ErrorActionPreference = 'Stop'
    $ProgressPreference = 'SilentlyContinue'
    $arch = if ($UseARM) { "arm64" } else { "x64" }
    $source = "https://nodejs.org/dist/v$Version/node-v$Version-$arch.msi"
    $destination = "$env:TEMP\node-$Version-$arch.msi"

    $InstallerArgs = "/i `"$destination`" /qn"
    if (-not [string]::IsNullOrWhiteSpace($InstallLocation)) {
        # Replace {version} in the installLocation with the current version if present
        $InstallLocation = $InstallLocation -replace "{version}", $Version
        $InstallerArgs += " INSTALLDIR=`"$InstallLocation`""
        Write-Host "Installing to $InstallLocation"
    }

    Write-Host "Downloading NodeJS"
    Write-Host "Source: $source"
    Write-Host "Destination: $destination"
    Write-Host "Downloading Node.js version $Version"

        Invoke-WebRequest $source -OutFile $destination
        Write-Host "Download Complete, checking signature of binary..."

        # Validate the signature of the downloaded binary
        $signature = Get-AuthenticodeSignature $destination
        if ($signature.Status -ne 'Valid') {
            throw "Could not validate the signature of $destination"
        }
    
    Write-Host "Signature validated, beginning installation..."

    Start-Process -FilePath msiexec -ArgumentList $InstallerArgs -Wait -NoNewWindow
    Write-Host "Node.js version $Version installation complete."
} catch {
    Write-Output $_
    Write-Error -Exception $_.Exception -ErrorAction Stop
}