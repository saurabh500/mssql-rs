#!/bin/bash

echo " Running Clippy " 
cargo clippy -- -D warnings

echo "Running format checker" 

cargo fmt --check
