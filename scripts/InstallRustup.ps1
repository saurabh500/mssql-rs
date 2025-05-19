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
}
catch {
    Write-Host "`nException while installing rustup, Error Message: " $_.Exception.Message
}