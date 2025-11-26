#!/bin/bash
set -e
source ~/.cargo/env

echo '==> Running clippy...'
./scripts/bclippy.sh
