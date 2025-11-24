#!/bin/bash
set -e

echo "Running cargo fmt on workspace..."
cargo fmt -- --check

echo "Running cargo fmt on mssql-py-core..."
cd mssql-py-core
cargo fetch
cargo fmt -- --check

echo "✓ All formatting checks passed!"
