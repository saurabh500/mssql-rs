#!/bin/bash
set -e

# Build all fuzz targets for mssql-tds.
# Requires: nightly toolchain and cargo-fuzz.

if ! rustup toolchain list | grep -q "nightly"; then
    echo "Installing nightly toolchain..."
    rustup toolchain install nightly
fi

if ! cargo +nightly fuzz --version &> /dev/null 2>&1; then
    echo "Installing cargo-fuzz..."
    cargo +nightly install cargo-fuzz
fi

cd "$(dirname "$0")/../mssql-tds"
cargo +nightly fuzz build
