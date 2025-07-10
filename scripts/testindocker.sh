#!/bin/bash

export PATH="$HOME/.cargo/bin:$PATH"

rustup component add llvm-tools-preview

cargo fetch
echo $FILTER
cargo llvm-cov nextest $FILTER --frozen --no-report --all-targets -p tds-x --no-fail-fast --profile ci --success-output immediate