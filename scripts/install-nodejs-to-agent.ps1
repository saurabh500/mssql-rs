# Install Node.js to agent externals folder
# This copies Node.js to where Azure DevOps agent expects to find it

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "CREATING NODE.JS IN AGENT EXTERNALS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Find Node.js dynamically in hostedtoolcache
$nodeSearchPath = "C:\hostedtoolcache\windows\node"
Write-Host "Searching for Node.js in: $nodeSearchPath" -ForegroundColor Yellow

$foundNode = Get-ChildItem -Path $nodeSearchPath -Filter "node.exe" -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $foundNode) {
    Write-Host "ERROR: No Node.js found in hostedtoolcache!" -ForegroundColor Red
    exit 1
}

$sourceNodeDir = $foundNode.DirectoryName
$sourceNodeExe = $foundNode.FullName
Write-Host "Found Node.js: $sourceNodeExe" -ForegroundColor Green

# Use hardcoded path for agent node24 bin directory
$targetNodeDir = "C:\vss-agent\4.268.0\externals\node24\bin"
$targetNodeExe = Join-Path $targetNodeDir "node.exe"
Write-Host "Target node directory: $targetNodeDir" -ForegroundColor Green

# Create target directory
Write-Host "Creating directory: $targetNodeDir" -ForegroundColor Cyan
New-Item -ItemType Directory -Force -Path $targetNodeDir | Out-Null

# Copy node.exe and related files
Write-Host "Copying Node.js files..." -ForegroundColor Cyan

# Copy all files from the node directory (node.exe, npm, npx, etc.)
Get-ChildItem -Path $sourceNodeDir -File | ForEach-Object {
    $destPath = Join-Path $targetNodeDir $_.Name
    Write-Host "  Copying: $($_.Name)" -ForegroundColor Gray
    Copy-Item -Path $_.FullName -Destination $destPath -Force
}

# Also copy node_modules if present (for npm)
$sourceNodeModules = Join-Path $sourceNodeDir "node_modules"
if (Test-Path $sourceNodeModules) {
    $targetNodeModules = Join-Path $targetNodeDir "node_modules"
    Write-Host "  Copying node_modules..." -ForegroundColor Gray
    Copy-Item -Path $sourceNodeModules -Destination $targetNodeModules -Recurse -Force
}

# Verify the copy worked
if (Test-Path $targetNodeExe) {
    Write-Host "SUCCESS: Node.js copied to agent externals!" -ForegroundColor Green
    $version = & $targetNodeExe --version
    Write-Host "  Version: $version" -ForegroundColor Green
} else {
    Write-Host "ERROR: Failed to copy Node.js!" -ForegroundColor Red
    exit 1
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "NODE.JS SETUP COMPLETE" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
