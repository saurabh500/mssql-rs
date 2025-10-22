# Enable Named Pipes and Shared Memory protocols for SQL Server
# This script enables Named Pipes and Shared Memory protocols via registry modification

param(
    [string]$InstanceName = "MSSQLSERVER",
    [string]$SqlVersion = "MSSQL17",
    [bool]$RestartService = $true
)

Write-Host "=== SQL Server Protocol Configuration ===" -ForegroundColor Cyan
Write-Host "Instance: $InstanceName" -ForegroundColor Yellow
Write-Host "SQL Version: $SqlVersion" -ForegroundColor Yellow

try {
    $computerName = $env:COMPUTERNAME
    
    # Determine registry paths based on instance name
    if ($InstanceName -eq "MSSQLSERVER") {
        $namedPipesPath = "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\$SqlVersion.MSSQLSERVER\MSSQLServer\SuperSocketNetLib\Np"
        $sharedMemoryPath = "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\$SqlVersion.MSSQLSERVER\MSSQLServer\SuperSocketNetLib\Sm"
        $serviceName = "MSSQLSERVER"
    } else {
        $namedPipesPath = "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\$SqlVersion.$InstanceName\MSSQLServer\SuperSocketNetLib\Np"
        $sharedMemoryPath = "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\$SqlVersion.$InstanceName\MSSQLServer\SuperSocketNetLib\Sm"
        $serviceName = "MSSQL`$$InstanceName"
    }
    
    Write-Host "`nRegistry Paths:" -ForegroundColor Cyan
    Write-Host "  Named Pipes: $namedPipesPath" -ForegroundColor White
    Write-Host "  Shared Memory: $sharedMemoryPath" -ForegroundColor White
    Write-Host "  Service Name: $serviceName" -ForegroundColor White
    
    $protocolsEnabled = @()
    
    # Enable Named Pipes
    Write-Host "`n--- Configuring Named Pipes ---" -ForegroundColor Yellow
    if (Test-Path $namedPipesPath) {
        try {
            # Get current value
            $currentValue = Get-ItemProperty -Path $namedPipesPath -Name "Enabled" -ErrorAction SilentlyContinue
            $currentStatus = if ($currentValue.Enabled -eq 1) { "Enabled" } else { "Disabled" }
            Write-Host "Current Named Pipes status: $currentStatus" -ForegroundColor White
            
            # Enable Named Pipes (1 = enabled, 0 = disabled)
            Set-ItemProperty -Path $namedPipesPath -Name "Enabled" -Value 1
            Write-Host "✅ Named Pipes enabled via registry" -ForegroundColor Green
            $protocolsEnabled += "Named Pipes"
            
        } catch {
            Write-Error "Failed to enable Named Pipes: $($_.Exception.Message)"
        }
    } else {
        Write-Warning "Named Pipes registry path not found: $namedPipesPath"
        Write-Host "This might indicate SQL Server is not installed or using a different version." -ForegroundColor Yellow
    }
    
    # Enable Shared Memory
    Write-Host "`n--- Configuring Shared Memory ---" -ForegroundColor Yellow
    if (Test-Path $sharedMemoryPath) {
        try {
            # Get current value
            $currentValue = Get-ItemProperty -Path $sharedMemoryPath -Name "Enabled" -ErrorAction SilentlyContinue
            $currentStatus = if ($currentValue.Enabled -eq 1) { "Enabled" } else { "Disabled" }
            Write-Host "Current Shared Memory status: $currentStatus" -ForegroundColor White
            
            # Enable Shared Memory (1 = enabled, 0 = disabled)
            Set-ItemProperty -Path $sharedMemoryPath -Name "Enabled" -Value 1
            Write-Host "✅ Shared Memory protocol enabled via registry" -ForegroundColor Green
            $protocolsEnabled += "Shared Memory"
            
        } catch {
            Write-Error "Failed to enable Shared Memory: $($_.Exception.Message)"
        }
    } else {
        Write-Warning "Shared Memory registry path not found: $sharedMemoryPath"
        Write-Host "This might indicate SQL Server is not installed or using a different version." -ForegroundColor Yellow
    }
    
    # Restart SQL Server service if protocols were enabled
    if ($protocolsEnabled.Count -gt 0 -and $RestartService) {
        Write-Host "`n--- Restarting SQL Server Service ---" -ForegroundColor Yellow
        Write-Host "Protocols enabled: $($protocolsEnabled -join ', ')" -ForegroundColor Green
        Write-Host "Restarting SQL Server service: $serviceName" -ForegroundColor Yellow
        
        try {
            # Check if service exists
            $service = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
            if ($service) {
                Restart-Service -Name $serviceName -Force
                Write-Host "✅ SQL Server service restarted successfully" -ForegroundColor Green
                
                # Wait for service to be running
                $timeout = 30
                $elapsed = 0
                do {
                    Start-Sleep -Seconds 1
                    $elapsed++
                    $service = Get-Service -Name $serviceName
                } while ($service.Status -ne 'Running' -and $elapsed -lt $timeout)
                
                if ($service.Status -eq 'Running') {
                    Write-Host "✅ Service is running and ready" -ForegroundColor Green
                } else {
                    Write-Warning "Service restart completed but status is: $($service.Status)"
                }
            } else {
                Write-Warning "SQL Server service '$serviceName' not found"
            }
        } catch {
            Write-Error "Failed to restart SQL Server service: $($_.Exception.Message)"
        }
    } elseif ($protocolsEnabled.Count -gt 0) {
        Write-Host "`n⚠️  Protocols enabled but service restart skipped" -ForegroundColor Yellow
        Write-Host "Manual restart required: Restart-Service -Name '$serviceName' -Force" -ForegroundColor Cyan
    }
    
    # Summary
    Write-Host "`n=== Configuration Summary ===" -ForegroundColor Cyan
    if ($protocolsEnabled.Count -gt 0) {
        Write-Host "✅ Successfully enabled: $($protocolsEnabled -join ', ')" -ForegroundColor Green
    } else {
        Write-Host "⚠️  No protocols were enabled (may already be enabled or paths not found)" -ForegroundColor Yellow
    }
    
} catch {
    Write-Error "Failed to enable SQL Server protocols: $($_.Exception.Message)"
    exit 1
}

Write-Host "=== SQL Server Protocol Configuration Complete ===" -ForegroundColor Cyan