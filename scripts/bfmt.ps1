# PowerShell script for running cargo fmt on workspace and mssql-py-core
# Windows equivalent of bfmt.sh

$ErrorActionPreference = "Stop"

Write-Host "Running cargo fmt on workspace..."
cargo fmt -- --check
if ($LASTEXITCODE -ne 0) {
    Write-Error "Workspace formatting check failed"
    exit $LASTEXITCODE
}

Write-Host "Running cargo fmt on mssql-py-core..."
Push-Location mssql-py-core
try {
    cargo fetch
    cargo fmt -- --check
    if ($LASTEXITCODE -ne 0) {
        Write-Error "mssql-py-core formatting check failed"
        exit $LASTEXITCODE
    }
}
finally {
    Pop-Location
}

Write-Host "✓ All formatting checks passed!"
