#!/bin/bash

ip addr 

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

# Enable and start SSH service
sudo apt install openssh-server -y
sudo systemctl enable ssh
sudo systemctl start ssh

# Create a new user for SSH login
# Check for openssl and install if not present
if ! command -v openssl &> /dev/null
then
    echo "OpenSSL not found, installing..."
    sudo apt install openssl -y
fi


# Create a new user for SSH login
SSH_USER="sshuser"
SSH_PASS=$(openssl rand -base64 16)

echo "Generated SSH password for $SSH_USER: $SSH_PASS"

if ! id "$SSH_USER" &>/dev/null; then
    sudo useradd -m -s /bin/bash "$SSH_USER"
    echo "$SSH_USER:$SSH_PASS" | sudo chpasswd
    sudo usermod -aG sudo "$SSH_USER"
    echo "User $SSH_USER created with password for SSH login."
else
    echo "User $SSH_USER already exists."
fi

# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
./install-ssh-ubuntu.sh

# Install az cli
if ! command -v az &> /dev/null
then
    echo "Azure CLI not found, installing..."
    curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
else
    echo "Azure CLI is already installed"
fi

# Check if PowerShell is installed
if ! command -v pwsh &> /dev/null
then
    echo "PowerShell not found, installing..."

    # Install PowerShell
    sudo apt install -y powershell
else
    echo "PowerShell is already installed"
fi

install_rustup() {
    # Install Rustup
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    "${SCRIPT_DIR}/install-rustup.sh"
}

if [[ "$1" != "--skip-rustup" ]]; then
    echo "Installing Rustup..."
    install_rustup
fi


echo "Home dir is $HOME"
echo "Current dir is $(pwd)"
echo "PATH is $PATH"


popd  


