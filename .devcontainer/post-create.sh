#!/bin/bash

# This script is executed after the devcontainer is created
cargo fetch

curl -o- https://fnm.vercel.app/install | bash

FNM_PATH="/home/vscode/.local/share/fnm"
if [ -d "$FNM_PATH" ]; then
  export PATH="$FNM_PATH:$PATH"
  eval "`fnm env`"
fi
fnm install 20
fnm use 20

corepack enable

# Setup Python virtual environment for mssql-py-core development
echo "Setting up Python virtual environment..."
python3 -m venv /workspaces/mssql-tds/myvenv
source /workspaces/mssql-tds/myvenv/bin/activate

# Install maturin (build tool) and dev dependencies from pyproject.toml
pip install --upgrade pip
pip install maturin
pip install -e "/workspaces/mssql-tds/mssql-py-core[dev]"

echo "Python venv ready at /workspaces/mssql-tds/myvenv"
echo "Run 'source /workspaces/mssql-tds/myvenv/bin/activate' to activate"
