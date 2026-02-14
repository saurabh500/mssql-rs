#!/bin/bash
set -e
source ~/.cargo/env

echo '==> Checking format...'
./scripts/bfmt.sh
