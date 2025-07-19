# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

FROM mcr.microsoft.com/devcontainers/rust:1-1-bullseye

USER vscode

# Install nightly toolchain to give access to nightly features for docs-site build.
RUN rustup toolchain install nightly

# Install cargo dependencies
RUN cargo install cargo-llvm-cov --version 0.6.16 --locked && cargo install cargo-nextest --version 0.9.99 --locked
