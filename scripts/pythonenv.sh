#!/usr/bin/env bash

if ! command -v python &> /dev/null; then
    echo "Python is not installed."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

pip install pipenv
pipenv install 

pipenv run maturin develop --manifest-path "$SCRIPT_DIR/../tdsx-python/Cargo.toml"