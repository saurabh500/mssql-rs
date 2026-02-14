#!/bin/bash
set -e
source ~/.cargo/env
export PATH="/root/.local/share/fnm:$PATH"
eval "$(fnm env --shell bash)"
fnm use 20
corepack enable

# Enable corepack and allow automatic downloads without prompting (required for CI)
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
corepack enable

cd mssql-js
yarn install
yarn build
ls lib/generated
# echo "Check for formatting"
# yarn format:check
echo "Compiling Typescript files"
yarn buildapi
