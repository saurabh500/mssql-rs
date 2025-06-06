#!/bin/sh


set -e
set -o pipefail

export PATH="$HOME/.cargo/bin:$PATH"

cd /workspace


cd tds-x
cargo build
cargo nextest archive --archive-file tdslib-nextest-musl.tar.zst 
mv tdslib-nextest-musl.tar.zst .. 
