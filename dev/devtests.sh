#!/bin/bash

# Check if cargo nextest is installed
if ! command -v cargo-nextest &> /dev/null && ! cargo nextest --version &> /dev/null; then
    echo "Error: cargo-nextest is not installed."
    echo "Please install it with: cargo install cargo-nextest"
    exit 1
fi

# Generate certificates required for tests
./scripts/generate_mock_tds_server_certs.sh

cargo nextest run -E "not (test(connectivity))" --all-targets -p mssql-tds --no-fail-fast --profile ci --success-output immediate
