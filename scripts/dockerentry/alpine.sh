#!/bin/sh

# Use apk for Alpine Linux
set -e

apk update
apk add --no-cache \
    ca-certificates \
    zstd \
    tar \
    gcc \
    g++ \
    make \
    pkgconfig \
    openssl-dev \
    openssl-libs-static \
    musl-dev \
    curl

# Install Rust
curl https://sh.rustup.rs -sSf | sh -s -- -y

export PATH="$HOME/.cargo/bin:$PATH"

cargo install cargo-nextest --version 0.9.96

cd /workspace

cargo nextest run -E 'not (test(connectivity))' --workspace-remap /workspace --archive-file "$ARCHIVE_NAME" --no-fail-fast --profile ci --success-output immediate