# Run no-protocol resolution integration tests
# Prerequisites: Local SQL Server on localhost:1433 with sa credentials

$env:SQL_PASSWORD = "YourPassword"
$env:DB_USERNAME = "sa"
$env:DB_HOST = "localhost"

Write-Host "Running no-protocol resolution integration tests..." -ForegroundColor Cyan
Write-Host ""

Write-Host "Test 1: Protocol fallback with Encryption=On" -ForegroundColor Yellow
cargo test test_protocol_fallback_order_with_encryption_on --test test_no_protocol_resolution -- --nocapture --test-threads=1

Write-Host ""
Write-Host "Test 2: Instance name error handling" -ForegroundColor Yellow
cargo test test_instance_name --test test_no_protocol_resolution -- --nocapture --test-threads=1

Write-Host ""
Write-Host "Test 3: Port priority" -ForegroundColor Yellow  
cargo test test_port_takes_priority --test test_no_protocol_resolution -- --nocapture --test-threads=1

Write-Host ""
Write-Host "Test 4: Localhost format variations" -ForegroundColor Yellow
cargo test test_various_localhost_formats --test test_no_protocol_resolution -- --nocapture --test-threads=1

Write-Host ""
Write-Host "Test 5: Edge cases" -ForegroundColor Yellow
cargo test test_whitespace_handling --test test_no_protocol_resolution -- --nocapture --test-threads=1
cargo test test_case_insensitive_protocol --test test_no_protocol_resolution -- --nocapture --test-threads=1

Write-Host ""
Write-Host "All tests complete!" -ForegroundColor Green
