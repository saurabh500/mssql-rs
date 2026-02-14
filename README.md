# mssql-rs

A Rust implementation of the [Tabular Data Stream (TDS)](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/) protocol used by Microsoft SQL Server. This library provides a foundational protocol layer to power SQL connectivity across multiple language bindings.

## Overview

The `mssql-tds` crate implements the TDS protocol from the ground up in Rust, providing a high-performance, memory-safe core that can be shared across driver ecosystems. The project is organized as a Cargo workspace:

| Crate | Purpose |
|---|---|
| `mssql-tds` | Core TDS protocol library |
| `mssql-js` | (Experimental) Node.js bindings via NAPI-RS |
| `mssql-tds-cli` | Interactive CLI client tool |
| `mssql-mock-tds` | Mock TDS server for testing |

## Getting Started

### Prerequisites

- [Rust](https://rustup.rs/) (version specified in `rust-toolchain.toml`)
- A C linker and OpenSSL development headers (e.g., `build-essential`, `libssl-dev` on Debian/Ubuntu)
- [cargo-nextest](https://nexte.st/) for running tests: `cargo install cargo-nextest --locked`
- Docker (optional, for running a local SQL Server instance)

### Clone and Build

```bash
git clone <repo-url>
cd mssql-rs
cargo build
```

### Git Hooks Setup

Install pre-commit hooks that run formatting and lint checks automatically:

```bash
./dev/setup-hooks.sh
```

## Local Build

Build the workspace:

```bash
cargo build
```

Before submitting changes, run the full check suite:

```bash
cargo bfmt       # Format check
cargo bclippy    # Lint (warnings are errors)
cargo btest      # Test with cargo-nextest
```

These aliases are defined in `.cargo/config.toml`. The `mssql-py-core` crate is excluded from the workspace and requires separate fmt/clippy runs via the scripts in `scripts/`.

## Contribute

This project is not currently accepting public pull requests. The repository is under active development by the Microsoft SQL Server drivers team.

# Central Feed Services (CFS) - Engineering Systems Standard Requirement
The project uses Rust (Cargo) crates and CFS onboarding required configuring this project to only consume packages through Azure Artifacts.
This is an engineering system standard which is required company wide.

The repo is configured to use Azure Artifacts as a package source.
There is an artifact feed RustTools in the project that is used to store the cargo crates.
The configuration is done in the `.cargo/config.toml` file.

### Developer Upstream (Unauthenticated Feed)
By default, the upstream crate source (`crates-io`) is replaced with the **ADO unauthenticated public feed** (`mssql-rs_Public`). This means day-to-day development—building and running tests—does **not** require any authentication or PAT setup.

However, if a dependency is added or an existing dependency version is changed, the new crate version must first be available in the feed. **Only an authenticated individual** with the appropriate permissions can import new crate versions into the feed. If you update dependencies and the build fails to resolve a crate, contact a team member with feed write access to import the required package.

> **Tip — testing with a new dependency locally:**
> To temporarily pull crates directly from crates.io before the new version is available in the upstream feed, remove (or comment out) the `replace-with` line in `.cargo/config.toml`:
> ```toml
> [source.crates-io]
> # replace-with = "mssql-rs_Public"   ← delete or comment out this line
> ```
> This lets Cargo resolve the updated dependency from crates.io so you can build and test locally. **Do not commit this change** — once the crate has been imported into the ADO feed, restore the `replace-with` line before submitting your PR.


# Next test goodness
I want to build the tests in one place, and run the binaries on a different machine/OS

On the source machine with source code, build the tests and archive them
` cd mssql-tds && cargo nextest archive --archive-file tdslib-nextest.tar.zst && mv tdslib-nextest.tar.zst ..`

Copy the archive file to a destination which has cargo and nextest installed, no source code needed. The tests can be executed using.
`cargo nextest run --archive-file tdslib-nextest.tar.zst`

Running inside docker container
`docker run -it --entrypoint bash -v "$PWD:/workspace" -e "DB_USERNAME=sa" -e "DB_HOST=sql1" -e "DB_PORT=1433" -e "SQL_PASSWORD=HappyPass1234" -e "ARCHIVE_NAME=tdslib-nextest.tar.zst" --network testnet ubuntu:24.04 /workspace/scripts/dockerentry/deb-bookworm.sh`


# Docker dev environment for linux


```sh
export DOCKER_BUILDKIT=1

# Get the Access token from windows using `azureauth ado token` and store the access token in a environment variable called 
# MSRUSTUP_ACCESS_TOKEN
# Then run the following build command to build the container.

# This needs to be done if any changes are being made to the Dockerfile, which havent been released to the ACR.

# Get an access token in Linux or Mac OS
export MSRUSTUP_ACCESS_TOKEN=$(azureauth ado token --mode all)

export DOCKER_BUILDKIT=1
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes

docker buildx create --use --name multiarch-builder
docker buildx inspect --bootstrap
docker buildx build --platform linux/amd64,linux/arm64 --progress=plain --debug  --secret id=MSRUSTUP_ACCESS_TOKEN,type=env --build-arg DEBUG=true -t tdslibrs.azurecr.io/ubuntu-msrustup:latest .

docker push tdslibrs.azurecr.io/ubuntu-msrustup:latest

# Run the container image.
docker run --rm -it --mount type=secret,id=MSRUSTUP_ACCESS_TOKEN --mount type=bind,source=/home/saurabh/work/RustPrototype,destination=/src   --env MSRUSTUP_ACCESS_TOKEN=$(cat MSRUSTUP_ACCESS_TOKEN)  tdslibrs.azurecr.io/ubuntu-msrustup:latest

# Publish to ACR


az acr login -n tdslibrs

docker push tdslibrs.azurecr.io/ubuntu-msrustup:latest
```


Get pat for local dev

```
azureauth ado pat --organization devdiv --display-name pathazauth --scope vso.packaging  --prompt-hint Hint1

```
```
azureauth ado token
```
