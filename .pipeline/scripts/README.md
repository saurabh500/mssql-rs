# SQL Server Configuration Scripts

This directory contains PowerShell scripts used by the Azure DevOps pipeline to configure SQL Server instances.

## Scripts

### Generate-SqlCertificates.ps1
Generates and installs self-signed certificates for SQL Server TLS encryption.

**Parameters:**
- `InstanceName` (optional): SQL Server instance name (default: "MSSQLSERVER")

**Usage:**
```powershell
.\Generate-SqlCertificates.ps1
.\Generate-SqlCertificates.ps1 -InstanceName "SQLDEV"
```

**What it does:**
- Creates a self-signed SSL certificate for SQL Server
- Configures certificate permissions for the SQL service account
- Installs the certificate in the SQL Server registry configuration
- Copies the certificate to the trusted root store
- Restarts SQL Server service to apply changes

### Enable-SqlBrowser.ps1
Enables and starts the SQL Server Browser service.

**Parameters:**
- `ServiceName` (optional): Name of the SQL Browser service (default: "SQLBrowser")

**Usage:**
```powershell
.\Enable-SqlBrowser.ps1
.\Enable-SqlBrowser.ps1 -ServiceName "SQLBrowser"
```

**What it does:**
- Checks if SQL Browser service exists
- Sets startup type to Automatic
- Starts the service if not running
- Provides detailed status information

### Enable-SqlProtocols.ps1
Enables Named Pipes and Shared Memory protocols for SQL Server via registry modification.

**Parameters:**
- `InstanceName` (optional): SQL Server instance name (default: "MSSQLSERVER")
- `SqlVersion` (optional): SQL Server version prefix (default: "MSSQL17")
- `RestartService` (optional): Whether to restart SQL Server service (default: $true)

**Usage:**
```powershell
.\Enable-SqlProtocols.ps1
.\Enable-SqlProtocols.ps1 -InstanceName "SQLDEV" -SqlVersion "MSSQL17" -RestartService $true
```

**What it does:**
- Enables Named Pipes protocol via registry
- Enables Shared Memory protocol via registry
- Optionally restarts SQL Server service to apply changes
- Provides detailed configuration status

## Pipeline Integration

These scripts are referenced in the Azure DevOps pipeline template:

```yaml
- task: PowerShell@2
  displayName: 'Generate Certificate for TLS encryption'
  inputs:
    targetType: 'filePath'
    filePath: '.pipeline/scripts/Generate-SqlCertificates.ps1'
    arguments: '-InstanceName "MSSQLSERVER"'

- task: PowerShell@2
  displayName: 'Enable SQL Browser service'
  inputs:
    targetType: 'filePath'
    filePath: '.pipeline/scripts/Enable-SqlBrowser.ps1'
    arguments: '-ServiceName "SQLBrowser"'

- task: PowerShell@2
  displayName: 'Enable Named Pipes and Shared Memory protocols'
  inputs:
    targetType: 'filePath'
    filePath: '.pipeline/scripts/Enable-SqlProtocols.ps1'
    arguments: '-InstanceName "MSSQLSERVER" -SqlVersion "MSSQL17" -RestartService $true'
```

## Prerequisites

- PowerShell with Administrator privileges
- SQL Server installed on the target machine
- Access to Windows registry for protocol configuration

## Error Handling

Both scripts include comprehensive error handling and will:
- Display clear status messages
- Exit with non-zero code on critical failures
- Provide troubleshooting information for common issues