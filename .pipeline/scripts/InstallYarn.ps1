$nodeVersion = node -v
Write-Host "Node.js version: $nodeVersion"
if (-not (Get-Command yarn -ErrorAction SilentlyContinue)) {
    Write-Host "Installing Yarn..."
    npm install --global yarn
    
    # Refresh PATH to pick up yarn
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
    
    # Also try to find yarn in npm's global prefix
    $npmPrefix = npm config get prefix
    $yarnPath = Join-Path $npmPrefix "yarn.cmd"
    
    if (Test-Path $yarnPath) {
        Write-Host "Found yarn at: $yarnPath"
        $yarnVersion = & $yarnPath -v
        Write-Host "Yarn version: $yarnVersion"
        Write-Host "##vso[task.prependpath]$npmPrefix"
    } else {
        Write-Host "Trying yarn from refreshed PATH..."
        $yarnVersion = yarn -v
        Write-Host "Yarn version: $yarnVersion"
    }
} else {
    Write-Host "Yarn is already installed."
    $yarnVersion = yarn -v
    Write-Host "Yarn version: $yarnVersion"
}
