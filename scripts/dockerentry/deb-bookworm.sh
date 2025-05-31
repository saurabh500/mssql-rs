#!/bin/bash

apt-get update && \
apt-get install -y \
    curl \
    ca-certificates \
    zstd \
    tar \
    build-essential \
    pkg-config \
    openssl


curl https://sh.rustup.rs -sSf | sh -s -- -y

export PATH="$HOME/.cargo/bin:$PATH"

cargo install cargo-nextest --version 0.9.96 --locked

cd /workspace

# Setup the certificate 

cp ca.crt /usr/local/share/ca-certificates
update-ca-certificates
# Verify the certificate
openssl verify -CAfile /etc/ssl/certs/ca-certificates.crt mssql.pem

cargo nextest run -E 'not (test(connectivity))' --workspace-remap /workspace --archive-file "$ARCHIVE_NAME" --no-fail-fast --profile ci --success-output immediate