#!/bin/sh


set -e

export PATH="$HOME/.cargo/bin:$PATH"

cd /workspace

# Detect architecture and set the appropriate musl target
ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
    RUST_TARGET="aarch64-unknown-linux-musl"
elif [ "$ARCH" = "x86_64" ]; then
    RUST_TARGET="x86_64-unknown-linux-musl"
else
    echo "Unsupported architecture: $ARCH"
    exit 1
fi

echo "==> Building for target: $RUST_TARGET"

# Ensure the musl target is installed
echo "==> Installing musl target: $RUST_TARGET"
rustup target add "$RUST_TARGET"

cd mssql-tds

# Build tests for musl WITHOUT gssapi feature to avoid dynamic linking to libkrb5
# The gssapi feature uses #[link(name = "gssapi_krb5", kind = "dylib")] which
# forces dynamic linking to glibc's libgssapi_krb5.so, making binaries incompatible
# with musl libc. Disabling gssapi produces fully static binaries.
#
# We only build the mssql-tds package (not --workspace) because workspace builds
# don't properly propagate --no-default-features to internal dependencies.
echo "==> Building tests without gssapi feature for static musl binary"
cargo nextest archive --target "$RUST_TARGET" --archive-file ../tdslib-nextest-musl.tar.zst -p mssql-tds --no-default-features
