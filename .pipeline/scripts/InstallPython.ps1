param(
    [string]$Version = "3.12.*"
)

# Check if Python is already installed
$pythonInstalled = $false
try {
    $pythonVersion = python --version 2>&1
    Write-Host "Python is installed: $pythonVersion"
    $pythonInstalled = $true
}
catch {
    Write-Host "Python is not installed"
}

if (-not $pythonInstalled) {
    Write-Host "Installing Python $Version..."
    
    # Get the latest Python patch version for the specified version
    $nugetUrl = "https://api.nuget.org/v3-flatcontainer/python/index.json"
    $response = Invoke-RestMethod -Uri $nugetUrl
    $latestVersion = $response.versions | Where-Object { $_ -like $Version -and $_ -notmatch '-' } | Sort-Object { [version]$_ } -Descending | Select-Object -First 1
    Write-Host "Latest Python $Version version: $latestVersion"
    
    # Download Python from GitHub actions/python-versions manifest
    $versionsManifest = Invoke-RestMethod "https://raw.githubusercontent.com/actions/python-versions/main/versions-manifest.json"
    $pythonAsset = $versionsManifest | Where-Object { $_.version -like $Version -and $_.stable -eq $true } | Select-Object -ExpandProperty files -Property version | Where-Object { $_.platform -eq "win32" -and $_.arch -eq "x64" } | Select-Object -First 1
    
    if ($pythonAsset) {
        Write-Host "Downloading Python $($pythonAsset.version) from $($pythonAsset.download_url)"
        $tempFile = Join-Path $env:TEMP "$([GUID]::NewGuid().ToString()).zip"
        $extractPath = Join-Path $env:TEMP "python-installer"
        
        Invoke-WebRequest -Uri $pythonAsset.download_url -OutFile $tempFile
        Write-Host "Extracting to $extractPath"
        New-Item -ItemType Directory -Path $extractPath -Force | Out-Null
        Expand-Archive -Path $tempFile -DestinationPath $extractPath -Force
        Remove-Item $tempFile
        
        # Run the setup script
        Write-Host "Running Python installation script..."
        Push-Location -Path $extractPath
        & .\setup.ps1
        Pop-Location
        
        Remove-Item -Path $extractPath -Recurse -Force -ErrorAction SilentlyContinue
        
        # Verify installation
        $pythonVersion = python --version
        Write-Host "Python installed successfully: $pythonVersion"
    }
    else {
        Write-Error "Could not find Python $Version in the versions manifest"
        exit 1
    }
}

# Install pipenv
Write-Host "Installing pipenv..."
pip install pipenv
