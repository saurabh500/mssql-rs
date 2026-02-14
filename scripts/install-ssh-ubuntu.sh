#!/bin/bash

# Enable and start SSH service
sudo apt update
sudo apt install openssh-server -y
sudo systemctl enable ssh
sudo systemctl start ssh

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