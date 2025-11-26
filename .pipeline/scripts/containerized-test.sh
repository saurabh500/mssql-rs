#!/bin/bash
set -e
source ~/.cargo/env

update-ca-certificates

echo '==> Running tests...'
mkdir -p /workspace/target/nextest/ci
cargo llvm-cov nextest "$@" --frozen --no-report --all-targets --package mssql-tds --no-fail-fast --profile ci --success-output immediate

echo '==> Generating coverage report...'
cargo llvm-cov report --package mssql-tds --lcov --output-path /workspace/target/lcov.info
