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
    Write-Error "Python is not installed. Python is expected to be pre-installed in the build image."
    exit 1
}

# Check if pip is accessible
Write-Host "Checking if pip is accessible..."
$pipAccessible = $false
try {
    $pipVersion = pip --version 2>&1
    Write-Host "pip is available: $pipVersion"
    $pipAccessible = $true
}
catch {
    Write-Host "pip is not accessible on PATH"
}

# If pip is not accessible, find Python location and add Scripts to PATH
if (-not $pipAccessible) {
    Write-Host "Locating python.exe to find Scripts directory..."
    
    try {
        $pythonExePath = (Get-Command python -ErrorAction Stop).Source
        $pythonDir = Split-Path $pythonExePath -Parent
        $scriptsPath = Join-Path $pythonDir "Scripts"
        
        Write-Host "Found Python at: $pythonDir"
        Write-Host "Scripts directory: $scriptsPath"
        
        if (Test-Path $scriptsPath) {
            # Add to current session PATH
            $env:Path = "$scriptsPath;$env:Path"
            
            # Set Azure DevOps pipeline variable for subsequent tasks
            Write-Host "##vso[task.prependpath]$scriptsPath"
            Write-Host "Added Scripts directory to PATH"
            
            # Verify pip is now accessible
            Start-Sleep -Seconds 1
            $pipVersion = pip --version
            Write-Host "pip is now available: $pipVersion"
        }
        else {
            Write-Error "Scripts directory does not exist at $scriptsPath"
            exit 1
        }
    }
    catch {
        Write-Error "Could not locate python.exe: $($_.Exception.Message)"
        exit 1
    }
}

# Install pipenv
Write-Host "Installing pipenv..."
pip install pipenv
