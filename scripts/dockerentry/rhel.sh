#!/bin/bash

# Use yum or dnf for RHEL-based systems instead of apt-get
set -e

dnf update -y || dnf update -y

dnf install -y \
    ca-certificates \
    zstd \
    tar \
    gcc \
    gcc-c++ \
    make \
    pkgconfig \
    openssl-devel

# Install Rust
curl https://sh.rustup.rs -sSf | sh -s -- -y

export PATH="$HOME/.cargo/bin:$PATH"

cargo install cargo-nextest --version 0.9.96 --locked

cd /workspace

cp ca.crt /etc/pki/ca-trust/source/anchors/

update-ca-trust extract

openssl verify -CAfile /etc/pki/tls/certs/ca-bundle.trust.crt mssql.crt

cargo nextest run -E 'not (test(connectivity))' --workspace-remap /workspace --archive-file "$ARCHIVE_NAME" --no-fail-fast --profile ci --success-output immediate