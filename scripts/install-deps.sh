#!/bin/bash

pushd /tmp  
wget -q https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt update

# Install the Azure CLI
sudo apt install azureauth -y

# Needed for msrustup download and essentials for building rust binaries.
sudo apt install jq unzip build-essential pkg-config libssl-dev -y

# Python stuff
sudo apt install python-is-python3 python3.10-venv pip -y

sudo apt install wget apt-transport-https software-properties-common -y

pip --version && pip install pipenv


# Check if PowerShell is installed
if ! command -v pwsh &> /dev/null
then
    echo "PowerShell not found, installing..."
    # Import the public repository GPG keys
    wget -q https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/packages-microsoft-prod.deb
    sudo dpkg -i packages-microsoft-prod.deb

    # Update the list of products
    sudo apt update

    # Install PowerShell
    sudo apt install -y powershell
else
    echo "PowerShell is already installed"
fi

# Install Rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
${SCRIPT_DIR}/install-rustup.sh

echo "Home dir is $HOME"
echo "Current dir is $(pwd)"
echo "PATH is $PATH"


popd  

