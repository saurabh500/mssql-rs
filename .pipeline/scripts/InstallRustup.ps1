$URL = "https://static.rust-lang.org/rustup/dist/x86_64-pc-windows-msvc/rustup-init.exe";
$FileName = [System.IO.Path]::GetFileName($URL)
$DestinationFile = Join-Path $env:Temp $FileName

try {
    Write-Host "Downloading rustup from $URL"
    $wc = New-Object -TypeName System.Net.WebClient
    $wc.DownloadFile($URL, $DestinationFile)}
catch {
    Write-Error -Message "Failed to download rustup: $_.Message"
}

try{
    Write-Host "Begin installation of rustup"
    Start-Process "$DestinationFile" -ArgumentList @("-y") -Wait -NoNewWindow
    Write-Host "End installation of rustup"
    
    # Add Cargo bin directory to PATH for subsequent tasks
    $cargoPath = "$env:USERPROFILE\.cargo\bin"
    Write-Host "Adding $cargoPath to PATH for subsequent tasks"
    
    # Update PATH for current session
    $env:Path = "$cargoPath;$env:Path"
    
    # Set Azure DevOps pipeline variable for subsequent tasks
    Write-Host "##vso[task.prependpath]$cargoPath"

    # Verify cargo is accessible
    Start-Sleep -Seconds 2
    & cargo --version
    Write-Host "Cargo is now available"
}
catch {
    Write-Host "`nException while installing rustup, Error Message: " $_.Exception.Message
}