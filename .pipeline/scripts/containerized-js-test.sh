#!/bin/bash
set -e

update-ca-certificates

source ~/.cargo/env
export PATH="/root/.local/share/fnm:$PATH"
eval "$(fnm env --shell bash)"
fnm use 20
corepack enable

# Enable corepack and allow automatic downloads without prompting (required for CI)
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
corepack enable

cd mssql-js
yarn testci > junit.xml
