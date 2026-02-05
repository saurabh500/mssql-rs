# Add Node.js to PATH for Azure DevOps tasks
# Using x64 Node.js via Windows ARM64 emulation since ARM64 Node isn't available

# Find Node.js dynamically in hostedtoolcache
$nodeSearchPath = "C:\hostedtoolcache\windows\node"
Write-Host "Searching for Node.js in: $nodeSearchPath" -ForegroundColor Yellow

$foundNodes = Get-ChildItem -Path $nodeSearchPath -Filter "node.exe" -Recurse -ErrorAction SilentlyContinue
if (-not $foundNodes) {
    Write-Host "ERROR: No Node.js found in hostedtoolcache!" -ForegroundColor Red
    exit 1
}

# Use the first one found
$nodeExe = $foundNodes[0].FullName
$nodePath = $foundNodes[0].DirectoryName

Write-Host "Found Node.js at: $nodeExe" -ForegroundColor Green

# Test that it works (via x64 emulation on ARM64)
try {
    $version = & $nodeExe --version
    $arch = & $nodeExe -e "console.log(process.arch)"
    Write-Host "Node.js version: $version (arch: $arch)" -ForegroundColor Green
    if ($arch -eq "x64") {
        Write-Host "Running via Windows ARM64 x64 emulation" -ForegroundColor Yellow
    }
} catch {
    Write-Host "ERROR: Node.js failed to execute: $_" -ForegroundColor Red
    exit 1
}

# Prepend to PATH for subsequent steps
Write-Host "Adding to PATH: $nodePath" -ForegroundColor Cyan
Write-Host "##vso[task.prependpath]$nodePath"

# Log agent externals info if available
$agentExternals = "$env:AGENT_HOMEDIRECTORY\externals"
if (Test-Path $agentExternals) {
    Write-Host "Agent externals directory: $agentExternals" -ForegroundColor Cyan
}

Write-Host "Node.js PATH setup complete!" -ForegroundColor Green
