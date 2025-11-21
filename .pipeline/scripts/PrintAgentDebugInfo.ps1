Write-Host "Current PATH:"
Write-Host $env:Path
Write-Host ""

$pythonDir = "C:\ProgramData\Tools\Python\3.12.10\x64"
if (Test-Path $pythonDir) {
    Write-Host "Directory $pythonDir exists"
    Write-Host "Recursive directory tree:"
    Get-ChildItem -Path $pythonDir -Recurse -Force | ForEach-Object {
        $relativePath = $_.FullName.Substring($pythonDir.Length)
        if ($_.PSIsContainer) {
            Write-Host "[DIR]  $relativePath"
        } else {
            Write-Host "[FILE] $relativePath ($($_.Length) bytes)"
        }
    }
} else {
    Write-Host "Directory $pythonDir does NOT exist"
}
