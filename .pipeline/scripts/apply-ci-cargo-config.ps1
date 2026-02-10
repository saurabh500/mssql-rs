# Apply CI-specific cargo configuration
# This script is called in CI pipelines to use authenticated ADO feeds

Write-Host "Applying CI cargo configuration..."
Copy-Item -Path .cargo\config.ci.toml -Destination .cargo\config.toml -Force
Write-Host "CI cargo config applied. Using authenticated ADO feeds."
