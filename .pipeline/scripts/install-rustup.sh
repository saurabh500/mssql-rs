#!/bin/bash

echo "Installing Rust..."
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
echo "##vso[task.prependpath]$HOME/.local/bin"
echo "##vso[task.prependpath]$HOME/.cargo/bin"
