# Enable SQL Browser Service
# This script checks and enables the SQL Server Browser service if it's not running

param(
    [string]$ServiceName = 'SQLBrowser'
)

Write-Host "=== SQL Browser Service Configuration ===" -ForegroundColor Cyan

try {
    # Check if SQL Browser service exists
    $browserService = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    
    if ($browserService) {
        Write-Host "SQL Browser service found: $($browserService.DisplayName)" -ForegroundColor Green
        Write-Host "Current status: $($browserService.Status)" -ForegroundColor Yellow
        Write-Host "Startup type: $($browserService.StartType)" -ForegroundColor Yellow
        
        if ($browserService.Status -ne 'Running') {
            Write-Host "SQL Browser service is not running. Enabling and starting it..." -ForegroundColor Yellow
            
            # Set startup type to Automatic
            Set-Service -Name $ServiceName -StartupType Automatic
            Write-Host "Set startup type to Automatic" -ForegroundColor Green
            
            # Start the service
            Start-Service -Name $ServiceName
            Write-Host "Starting SQL Browser service..." -ForegroundColor Yellow
            
            # Wait a moment and check status
            Start-Sleep -Seconds 2
            $updatedService = Get-Service -Name $ServiceName
            
            if ($updatedService.Status -eq 'Running') {
                Write-Host "✅ SQL Browser service started successfully" -ForegroundColor Green
            } else {
                Write-Warning "SQL Browser service failed to start. Status: $($updatedService.Status)"
                exit 1
            }
        } else {
            Write-Host "✅ SQL Browser service is already running" -ForegroundColor Green
        }
        
        # Display final status
        $finalService = Get-Service -Name $ServiceName
        Write-Host "`nFinal Status:" -ForegroundColor Cyan
        Write-Host "  Status: $($finalService.Status)" -ForegroundColor White
        Write-Host "  Startup Type: $($finalService.StartType)" -ForegroundColor White
        
    } else {
        Write-Warning "SQL Browser service not found on this system"
        Write-Host "This might indicate SQL Server is not installed or Browser service is not available" -ForegroundColor Yellow
        exit 1
    }
    
} catch {
    Write-Error "Failed to configure SQL Browser service: $($_.Exception.Message)"
    exit 1
}

Write-Host "=== SQL Browser Service Configuration Complete ===" -ForegroundColor Cyan