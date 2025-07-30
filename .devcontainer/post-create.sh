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
