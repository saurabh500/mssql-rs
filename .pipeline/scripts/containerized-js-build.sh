#!/bin/bash
set -e
source ~/.cargo/env
export PATH="/root/.local/share/fnm:$PATH"
eval "$(fnm env)"
fnm use 20

cd mssql-js
yarn install
yarn build
ls lib/generated
echo "Check for formatting"
yarn format:check
echo "Compiling Typescript files"
yarn buildapi
