#!/bin/bash

export FNM_PATH="$HOME/.local/share/fnm"

export PATH="$FNM_PATH:$PATH"
eval "`fnm env`"

echo $PATH

ls /home/cloudtest/.local/share/fnm

ls /home/cloudtest/.local/share/fnm/fnm
# /home/cloudtest/.local/share/fnm/fnm install 20
# /home/cloudtest/.local/share/fnm/fnm use 20
# npm install --global yarn
# Download and install Node.js:
fnm install 20

fnm use 20
npm install --global yarn

corepack enable