param (
    $Updates,
    $AddFirewallExceptions,
    $EnableMixedMode,
    $InstanceName = "MSSQLSERVER",
    $UpdateOnly,
    $ConfigurationFile
)

$ErrorActionPreference = 'Stop';
. $PSScriptRoot\CloudTest-Common.ps1

function Install-windows-sql-server-2022-Dev($ConfigurationFile) {
    # parameters
    $url = "https://go.microsoft.com/fwlink/p/?linkid=2215158";
    
    # download
    $file = DownloadFile $url 'SQL2022-SSEI-Dev.exe' -validateSignature;
    
    # install
    Write-Host 'Installing SQL Server';
    $arg = " /ConfigurationFile=`"$ConfigurationFile`" /QUIET /IAcceptSQLServerLicenseTerms ";

    Write-Host "Install start: $file $arg";
    Start-Process "$file" -ArgumentList $arg -Wait -NoNewWindow;
    
    Write-Host 'Install Done.';
}

function Update-SQL-Server-2022-CU ($InstanceName) {
    Write-Host "Downloading CU17 update for Sql Server 2022"
    $DownloadUrl = "https://download.microsoft.com/download/9/6/8/96819b0c-c8fb-4b44-91b5-c97015bbda9f/SQLServer2022-KB5048038-x64.exe"
    $UpdateFile = DownloadFile $DownloadUrl 'SQLServer2022-KB5048038-x64.exe' -validateSignature

    # install
    Write-Host "Installing CU update for Sql Server 2022"
    $arg = " /q /IAcceptSQLServerLicenseTerms /Action=Patch /InstanceName=$InstanceName"
    Start-Process "$UpdateFile" -ArgumentList $arg -Wait -NoNewWindow
    Write-Host "Install done."
}

function Update-SQL-Server-2022-GDR ($InstanceName) {
    Write-Host "Downloading CU15 + GDR update for Sql Server 2022"
    $downloadUrl = "https://download.microsoft.com/download/1/f/6/1f6fb0b0-6122-42e0-bc45-265a1cf439a4/SQLServer2022-KB5046862-x64.exe"
    $UpdateFile = DownloadFile $downloadUrl 'SQLServer2022-KB5046862-x64.exe' -validateSignature

    # install
    Write-Host "Installing CU15 + GDR update for Sql Server 2022"
    $arg = " /q /IAcceptSQLServerLicenseTerms /Action=Patch /InstanceName=$InstanceName"
    Start-Process "$UpdateFile" -ArgumentList $arg -Wait -NoNewWindow
    Write-Host "Install done."
}

function Open-FirewallForSQLServer {
    Write-Host "Open SQL ports in the firewall";
    & netsh advfirewall firewall add rule name="__enable-inbound-tcp-connection-sql-server" dir=in action=allow protocol=TCP localport=1433;
    & netsh advfirewall firewall add rule name="__enable-inbound-udp-connection-sql-server" dir=in action=allow protocol=UDP localport=1434;

}
function Enable-MixedAuthMode {
    Write-Host "Enable mixed mode for server authentication (SQL Server and Windows Auth mode)";
    Set-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQLServer" -Name LoginMode -Value 2
    Write-Host "Mixed mode for server authentication has changed as below:"
    Get-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQLServer" -Name LoginMode
}

function Set-NewInstanceName ($InstanceName, $ConfigurationFile) {
    $ConfigFile = Get-Content "$ConfigurationFile"
    $NewContent = $ConfigFile -replace ("^INSTANCENAME=.*$", "INSTANCENAME=$InstanceName")
    $NewContent | Set-Content -Path "$ConfigurationFile"
}

$ScriptDir = $PSScriptRoot;
if (!$ConfigurationFile) {
    # Configuration file is not provided, use the default one
    $ConfigurationFile = "$ScriptDir\SqlServer22InstallConfigurationFile.ini"
}

if($InstanceName -ne "MSSQLSERVER")
{
    Write-Host "Setting instance name to $InstanceName"
    Set-NewInstanceName -InstanceName $InstanceName -ConfigurationFile $ConfigurationFile
}

if($UpdateOnly)
{
    Update-SQL-Server-2022-CU -InstanceName $InstanceName
    Update-SQL-Server-2022-GDR -InstanceName $InstanceName
    exit
}

Install-windows-sql-server-2022-Dev -ConfigurationFile $ConfigurationFile;

if ($Updates)
{
    Update-SQL-Server-2022-CU -InstanceName $InstanceName
    Update-SQL-Server-2022-GDR -InstanceName $InstanceName
}
if ($AddFirewallExceptions)
{
    Open-FirewallForSQLServer
}

if ($EnableMixedMode)
{
    Enable-MixedAuthMode
}