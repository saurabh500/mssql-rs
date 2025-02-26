########## common ##########
$ErrorActionPreference = 'Stop';

function log([string]$message) {
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $message";
}

function logHeader([string]$message) {
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ********** $message" -ForegroundColor 'Cyan';
}

function AddFolderToMachinePath ([string]$folder) {
    # update system path
    $systemPath = [Environment]::GetEnvironmentVariable('PATH', [System.EnvironmentVariableTarget]::Machine);
    if(!$systemPath.Contains(";$folder;") -and !$systemPath.EndsWith(";$folder")) {
        log "SystemPath add [;$folder]";
        [Environment]::SetEnvironmentVariable("PATH", "$systemPath;$folder", [System.EnvironmentVariableTarget]::Machine);
    }
    else {
        log "SystemPath add [;$folder] skipped...";
    }
    
    # update local user path
    $userPath = $env:Path;
    if(!$userPath.Contains(";$folder;") -and !$userPath.EndsWith(";$folder")) {
        log "UserPath add [;$folder]";
        $env:Path = "$userPath;$folder";
    }
    else {
        log "UserPath add [;$folder] skipped...";
    }
}

function GetExePath([string]$exeName, [string]$defaultPath=$null) { 
    $exePath = Get-Command $exeName -ErrorAction SilentlyContinue;
    if($exePath) { return $exePath[0].Source; }
    else { return $defaultPath }
}

function ValidateSignature {
    param (
        [string]$filePath
    )
    log "Validating file signature..."
    $signatureStatus = (Get-AuthenticodeSignature $filePath).Status
    if ($signatureStatus -ne "Valid") {
        log "Invalid digital signature!"
        return $false
    }
    log "Valid signature"
    return $true
}

function Get-HumanReadableFileSize {
    param (
        [string]$filePath
    )

    try {
        if (Test-Path $filePath) {
            $fileSize = (Get-Item $filePath).Length
            $sizeUnits = "Bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"
            $sizeIndex = 0

            while ($fileSize -ge 1024 -and $sizeIndex -lt $sizeUnits.Length - 1) {
                $fileSize = $fileSize / 1024
                $sizeIndex++
            }

            return "{0:N2} {1}" -f $fileSize, $sizeUnits[$sizeIndex]
        } else {
            log "Could not determine file size. The specified file path does not exist: $filePath"
            return "Unknown"
        }
    }
    catch {
        log "An error occurred while determining the file size for the path: $filePath. Error: $_"
        return "Unknown"
    }
}

function DownloadFile {
    param (
        [string]$fileUrl,
        [string]$downloadFileName,
        [bool]$useBitsTransfer = $false, # use false, as BitsTransfer not supported in 1ES Image Factory
        [switch]$validateSignature = $false
    )

    $downloadFilePath = "$env:TEMP\$downloadFileName"
    log "Download [$fileUrl]->[$downloadFilePath];"

    if ([IO.File]::Exists($downloadFilePath) -and "$env:ForceDownloadFile" -ne 'true') {
        if ($validateSignature) {
            if (ValidateSignature -filePath $downloadFilePath) {
                log "Download skipped. File exists with a valid signature. Set ForceDownloadFile=true to force download."
                return $downloadFilePath
            }
            else {
                log "File exists but signature was invalid! Downloading again."
            }
        }
        else {
            log "Download skipped. File exists. Set ForceDownloadFile=true to force download."
            return $downloadFilePath
        }
    }

    $start_time = Get-Date
    if ($useBitsTransfer) {
        Import-Module BitsTransfer
        Start-BitsTransfer -Source $fileUrl -Destination $downloadFilePath -RetryInterval 60 -RetryTimeout 180
    }
    else {
        (New-Object System.Net.WebClient).DownloadFile($fileUrl, $downloadFilePath)
    }

    $fileSize = Get-HumanReadableFileSize $downloadFilePath
    log "Download completed. Time taken: $((Get-Date).Subtract($start_time).Seconds) second(s); File size: $fileSize;"

    if ($validateSignature) {
        if (-not (ValidateSignature -filePath $downloadFilePath)) {
            throw "Could not validate signature of $downloadFilePath"
        }
    }

    return $downloadFilePath
}

function Install-MSI ($msiFile) {
    $logFileName = [System.IO.Path]::GetFileName($msiFile);
    $logFile = "$($env:temp)\$($logFileName)_install.log";
    $args = @("/i", "`"$file`"", "/quiet", "/norestart", "/L*i", "`"$logFile`"");
    log "Install MSI [$msiFile]. Logs @[$logFile]";
    $process = Start-Process msiexec.exe -ArgumentList $args -Wait -NoNewWindow;
    $log = [System.IO.File]::ReadAllText($logFile);
    log "*** Install logs: $log";
    if(!($log -like '*success or error status: 0.*')) {
        log "Install MSI failed [$msiFile]" -ErrorAction Stop;
    }
}

function RetryUntilTrue([System.Func[bool]] $method, [int]$timeoutInSec, [string]$messageId) {
    log "[$messageId] RetryUntilTrue timeout=$timeoutInSec sec";
 
    $timer = [Diagnostics.Stopwatch]::StartNew();
    while ($timer.Elapsed.TotalSeconds -lt $timeoutInSec) {
        
        $ErrorActionPreference = 'SilentlyContinue'; # to avoid changing LASTEXITCODE and failure status

        try {
            $response = $method.Invoke();
            if($response) {
                $ErrorActionPreference = 'Stop'; # reset state
                return;
            }
        } catch {
            log "[$messageId][exception] message: $($_.Exception.Message);";
        }

        $ErrorActionPreference = 'Stop'; # reset state
        
        log "[$messageId] retrying... elapsed=$($timer.Elapsed); timeoutInSec=$timeoutInSec;";
        Start-Sleep -Seconds 5;
    }

    throw "[$messageId] Timeout occurred";
}

function Download-BlobContainer([string]$blobContainerUrl, [string]$downloadPath) {
    logHeader "Download-BlobContainer blobContainerUrl=$blobContainerUrl; downloadPath=$downloadPath";

    if(![IO.Directory]::Exists($downloadPath)) {
        log "create directory $downloadPath";
        [IO.Directory]::CreateDirectory($downloadPath);
    }

    $wc = New-Object System.Net.WebClient;
    $downloadStr = $wc.DownloadString($blobContainerUrl);
    $xml = [xml]$downloadStr;
    $blobItems = $xml.EnumerationResults.Blobs.Blob | ForEach-Object {
        New-Object -TypeName PSObject -Property @{
            BlobName = $_.Name
            BlobUrl = $_.Url
            BlobType = $_.Properties.BlobType
        }
    };
    
    foreach($blobItem in $blobItems) {
        log "downloading blobItem=$blobItem";
        $blobLocalFile = [IO.Path]::Combine($downloadPath, $blobItem.BlobName);
        $wc.DownloadFile($blobItem.BlobUrl, $blobLocalFile);
    }
}

function Download-BlobContainer-MI([string]$accountName, [string]$containerName, [string]$clientId, [string]$downloadPath){
    logHeader "Download-BlobContainer-MI accountName=$accountName; containerName=$containerName; clientId=$clientId; downloadPath=$downloadPath";

    if(![IO.Directory]::Exists($downloadPath)) {
        log "create directory $downloadPath";
        [IO.Directory]::CreateDirectory($downloadPath);
    }
   
    #Install Modules to allow PS to interact with Azure and Azure Blob 
    if ($null -eq (Get-InstalledModule -Name "Az.Storage" -MinimumVersion 3.3.0 -ErrorAction SilentlyContinue))
    {
        Write-Host "Install Az.Storage"
        Install-Module -Name Az.Storage -RequiredVersion 3.3.0 -Force -AllowClobber
    }
    else
    {
        Write-Host "Module Az.Storage exists, skip install"
    }

    Import-Module Az.Storage

    Connect-AzAccount -Identity -AccountId $clientid
    $ctx = New-AzStorageContext -StorageAccountName $accountName -UseConnectedAccount
    Get-AzStorageBlob -Container $containerName -Blob '*' -Context $ctx | Get-AzStorageBlobContent -Destination $downloadPath
}


########## common-tools ##########

function Chocolatey-Install {
    logHeader "Chocolatey installation started; LASTEXITCODE=$LASTEXITCODE"
    $startTime=Get-Date;
    $chocoExePath = "$env:ProgramData\Chocolatey\bin"

    if ($($env:Path).ToLower().Contains($($chocoExePath).ToLower())) {
        log "Chocolatey found in PATH, skipping install..."
        return;
    }

    AddFolderToMachinePath $chocoExePath;

    # Run the installer
    Set-ExecutionPolicy Bypass -Scope Process -Force
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072

    $installerUrl = 'https://chocolatey.org/install.ps1'
    $installerPath = "$env:TEMP\install.ps1"

    # Download the installer script
    Invoke-WebRequest -Uri $installerUrl -OutFile $installerPath

    # Validate the signature of the downloaded script
    $signature = Get-AuthenticodeSignature $installerPath
    if ($signature.Status -ne 'Valid') {
        throw "Could not validate the signature of $installerPath"
    }

    # Execute the installer script
    $global:LASTEXITCODE = 0
    & $installerPath

    if ($LASTEXITCODE -eq 3010) {
        Write-Host 'The recent changes indicate a reboot is necessary. Please reboot at your earliest convenience.'
    }

    # Turn off confirmation
    choco feature enable -n allowGlobalConfirmation

    # Turn off progress of download
    choco feature disable -n=showDownloadProgress

    refreshenv;
    log "Chocolatey installation completed; LASTEXITCODE=$LASTEXITCODE; Total time taken = $($(Get-Date) - $startTime)"
}
