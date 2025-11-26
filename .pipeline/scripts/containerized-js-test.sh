#!/bin/bash
set -e

update-ca-certificates

source ~/.cargo/env
export PATH="/root/.local/share/fnm:$PATH"
eval "$(fnm env --shell bash)"
fnm use 20

cd mssql-js
yarn testci > junit.xml
