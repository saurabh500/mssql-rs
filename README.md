[Rust in Microsoft]: https://aka.ms/rust
[MSRustup from MS]: https://aka.ms/msrustup
[Personal Access Tokens]: https://sqlclientdrivers.visualstudio.com/_usersSettings/tokens
[Connect to feed]: https://sqlclientdrivers.visualstudio.com/SqlDevX/_artifacts/feed/RustTools/connect
[Rust build pipeline]: https://dev.azure.com/SqlClientDrivers/SqlDevX/_build?definitionId=1869

# Rust prototype project
The Rust prototype project is intended to be a starting and learning point for a new TDS library project.
It allows to try, and explore features, components, and techniques for TDS implementation in Rust.
The repo is not intended to be a production ready code, but rather a playground for learning and exploration.

## Getting Started
Prototype projects are developed in Rust language; therefore, the dev environment will require IDE and Rust toolchain. More details on installation are available further in the document.

[Rust in Microsoft] article is the great overview of guidelines and learning resources for any Rust developer. It is highly recommended as reading material.

### Tools and Prerequisites
These tools made the Rust development easier:
- Visual Studio Code 
- Extensions for Visual Studio Code
  - rust-analyzer
  - C/C++ - Visual Studio Marketplace
  - CodeLLDB - Visual Studio Marketplace
- Download Rust MS internal setup (msrustup.exe) from [MSRustup from MS]

#### Setting up the environment
To get a development environment running, please do the following:
1. Clone *RustPrototype* repo by running

```powershell
git clone https://sqlclientdrivers.visualstudio.com/SqlDevX/_git/RustPrototype
```

2. Navigate to the enlistment root directory and run the msrustup.exe command:
```powershell
msrustup.exe toolchain install
```

3. RustPrototype repo is configured to use Azure Artifacts as a package source and the build environment requires a login to the Azure Artifacts.
Repo is already set up with proper path, but it is necessary to generate *Personal Access Token* which allows access to the mirror.
To do that, navigate to [Personal Access Tokens] and create a new token which will include *Packaging read & write* scopes.
Make sure to copy the PAT token because you will need it later.

4. Open PowerShell at the root of the project folder and run:

```powershell
"Basic " + [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("PAT:" + (Read-Host -MaskInput "Enter PAT"))) | cargo login --registry RustTools
```
This script will prompt for PAT, paste the token generated in the previous step and press Enter.
In case to setup other environments, it is possible to find more instructions [Connect to feed],
and select Cargo on that page.

## Local build
To build the project locally, run the following command in the project root directory:

```
cargo build
```

## Build pipeline

There is a build pipeline in the Azure DevOps that is configured to build the project.
The pipeline can be triggered manually from this link:

[Rust build pipeline]

## Test
TODO: Describe and show how to run the tests for your software.

## Contribute
Rust prototype repo is for experimenting and learning.
There are Rust projects located in `prototype` directory.
When creating a new prototype, please create a new directory in the `prototype` and add a README.md file with the description of the new prototype.

Ensure builds are still successful prior to submitting the pull request.

# Central Feed Services (CFS) - Engineering Systems Standard Requirement
The project uses Rust (Cargo) crates and CFS onboarding required configuring this project to only consume packages through Azure Artifacts.
This is an engineering system standard which is required company wide.

The repo is configured to use Azure Artifacts as a package source.
There is an artifact feed RustTools in the project that is used to store the cargo crates.
The configuration is done in the `.cargo/config.toml` file.
