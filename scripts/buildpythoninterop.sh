#!/usr/bin/env bash

export PYTHON_HOME=$(python -c "import sys; print(sys.prefix)")

if ! command -v maturin &> /dev/null
then
    echo "maturin not found, installing..."
    cargo install maturin
fi
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "Home directory: $HOME"
echo "Python home: $PYTHON_HOME"
echo "Script directory: $SCRIPT_DIR"
ls $HOME


ls /home/cloudtest/.local/bin

# On some CI VMs, pipenv is not on the PATH
if ! command -v pipenv &> /dev/null
then
    echo "pipenv not found on PATH, using fully qualified name..."
    PIPENV_CMD="/home/cloudtest/.local/bin/pipenv"
else
    PIPENV_CMD="pipenv"
fi
echo "Using pipenv command: $PIPENV_CMD"
$PIPENV_CMD run pip install patchelf

# TODO: Uncomment this when we have the project ready and setup.
# $PIPENV_CMD run maturin build --frozen --manifest-path "$SCRIPT_DIR/../tdsx-python/Cargo.toml"

