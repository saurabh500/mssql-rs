#!/bin/bash

export FNM_PATH="$HOME/.local/share/fnm"

export PATH="$FNM_PATH:$PATH"

eval "`fnm env`"

echo $PATH

fnm install 20

fnm use 20
npm install --global yarn

corepack enable