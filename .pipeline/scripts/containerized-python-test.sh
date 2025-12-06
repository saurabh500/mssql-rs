#!/bin/bash
set -e

# Update CA certificates in container
update-ca-certificates

# Source cargo environment
source ~/.cargo/env

# Set Python path
export PATH="/usr/local/bin:$PATH"

# Run Python tests using the dev script
echo "Running Python tests for mssql-py-core..."
cd /workspace
./dev/test-python.sh

echo "Python tests completed successfully"
