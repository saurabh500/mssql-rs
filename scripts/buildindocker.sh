#!/bin/bash

export PATH="$HOME/.cargo/bin:$PATH"
pwd
ls
echo $PATH
rm -rf target
rustup component add llvm-tools

cargo fetch 

cargo build --frozen 2>&1

cargo build --frozen --release 2>&1

cargo bclippy

cargo fmt

