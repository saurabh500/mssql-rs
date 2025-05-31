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

cargo install cargo-nextest --version 0.9.96 --locked

cd /workspace


cd tds-x
cargo build
cargo nextest archive --archive-file tdslib-nextest-musl.tar.zst && mv tdslib-nextest-musl.tar.zst .. 
