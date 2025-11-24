#!/bin/bash
set -e

echo "Running cargo clippy on workspace..."
cargo clippy --workspace --frozen --all-features --all-targets -- -D warnings

echo "Running cargo clippy on mssql-py-core..."
cd mssql-py-core
cargo clippy --frozen --all-features --all-targets -- -D warnings

echo "✓ All clippy checks passed!"
