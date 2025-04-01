#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pushd $SCRIPT_DIR/..
cargo build && pipenv run maturin develop --manifest-path "$SCRIPT_DIR/../tdsx-python/Cargo.toml"
popd 