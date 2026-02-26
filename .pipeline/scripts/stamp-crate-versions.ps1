# stamp-crate-versions.ps1
# Patches Cargo.toml version fields for publishable crates before cargo publish.
# Uses the same versioning scheme as the NuGet wheel packaging.
#
# Usage (in pipeline):
#   pwsh .pipeline/scripts/stamp-crate-versions.ps1 -BuildReason "$(Build.Reason)" -BuildId "$(Build.BuildId)" -IsOfficial $false
#
# Usage (local testing):
#   pwsh .pipeline/scripts/stamp-crate-versions.ps1 -BuildReason "IndividualCI" -BuildId "99999" -IsOfficial $false -WhatIf

param(
    [Parameter(Mandatory=$true)]
    [string]$BuildReason,

    [Parameter(Mandatory=$true)]
    [string]$BuildId,

    [Parameter(Mandatory=$false)]
    [bool]$IsOfficial = $false,

    [Parameter(Mandatory=$false)]
    [string[]]$Crates = @("mssql-tds", "mssql-mock-tds"),

    [Parameter(Mandatory=$false)]
    [switch]$WhatIf
)

$ErrorActionPreference = 'Stop'

# Read base version from the first crate
$cargoTomlPath = Join-Path $PSScriptRoot "../../$($Crates[0])/Cargo.toml"
$cargoToml = Get-Content $cargoTomlPath -Raw
# Match the first `version = "..."` at the start of a line (multiline mode).
# This targets the [package] version, which appears before any dependency versions.
if ($cargoToml -match '(?m)^version\s*=\s*"([^"]+)"') {
    $baseVersion = $Matches[1]
} else {
    Write-Error "Could not extract version from $cargoTomlPath"
    exit 1
}

$dateStamp = Get-Date -Format "yyyyMMdd"

# Build.Reason is set by Azure Pipelines to indicate how the run was triggered:
#   Schedule      - cron-scheduled run          -> nightly prerelease
#   Manual        - user clicked "Run pipeline" -> release (official) or dev (non-official)
#   IndividualCI  - push to a monitored branch  -> dev prerelease
#   BatchedCI     - batched push trigger         -> dev prerelease
#   PullRequest   - PR validation (publish is skipped at the stage level)
switch ($BuildReason) {
    'Schedule' {
        $crateVersion = "$baseVersion-nightly.$dateStamp"
        Write-Host "Nightly build detected"
    }
    'Manual' {
        if ($IsOfficial) {
            $crateVersion = $baseVersion
            Write-Host "Official release build detected"
        } else {
            $crateVersion = "$baseVersion-dev.$dateStamp.$BuildId"
            Write-Host "Non-official manual build detected"
        }
    }
    default {
        $crateVersion = "$baseVersion-dev.$dateStamp.$BuildId"
        Write-Host "CI build detected (reason: $BuildReason)"
    }
}

Write-Host "Base version: $baseVersion"
Write-Host "Crate version: $crateVersion"

foreach ($crate in $Crates) {
    $path = Join-Path $PSScriptRoot "../../$crate/Cargo.toml"
    $path = Resolve-Path $path
    $content = Get-Content $path -Raw

    # Replace only the first version = "..." line (the [package] version)
    $updated = $content -replace '(?m)^(version\s*=\s*)"[^"]+"', "`$1`"$crateVersion`""

    if ($content -eq $updated) {
        Write-Warning "No version field found or already at target in $path"
        continue
    }

    if ($WhatIf) {
        Write-Host "[WhatIf] Would patch $path -> version = `"$crateVersion`""
    } else {
        Set-Content $path $updated -NoNewline
        Write-Host "Patched $path -> version = `"$crateVersion`""
    }
}

# Set pipeline variable for downstream steps
if (-not $WhatIf) {
    Write-Host "##vso[task.setvariable variable=crateVersion]$crateVersion"
    Write-Host "##vso[task.setvariable variable=crateVersion;isOutput=true]$crateVersion"
}

Write-Host "Done. Crate version: $crateVersion"
