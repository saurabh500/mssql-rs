#!/bin/bash
set -e
source ~/.cargo/env

# Update CA certificates in container
update-ca-certificates

# Verify certificate
openssl verify -CAfile /etc/ssl/certs/ca-certificates.crt /workspace/mssql.crt || true

# Fetch dependencies
echo '==> Fetching crates...'
cargo fetch

# Fetch mssql-py-core dependencies (it's outside workspace)
echo '==> Fetching mssql-py-core crates...'
cd mssql-py-core
cargo fetch
cd ..

# Build based on BUILD_TYPE
if [ "$BUILD_TYPE" = "Debug" ] || [ "$BUILD_TYPE" = "Both" ]; then
  echo '==> Building debug...'
  cargo build --frozen
fi

if [ "$BUILD_TYPE" = "Release" ] || [ "$BUILD_TYPE" = "Both" ]; then
  echo '==> Building release...'
  cargo build --frozen --release
fi

# Archive nextest (used by later test stages)
echo '==> Creating nextest archive...'
cd mssql-tds
cargo nextest archive --archive-file tdslib-nextest.tar.zst
mv tdslib-nextest.tar.zst /workspace/
cd ..
