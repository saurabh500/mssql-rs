#!/bin/bash

# This script installs gpg and other software for Git on WSL.

pushd /tmp

if ! command -v wget &> /dev/null; then
    echo "wget not found. Installing wget..."
    sudo apt update
    sudo apt install -y wget
fi
wget https://github.com/git-ecosystem/git-credential-manager/releases/download/v2.6.1/gcm-linux_amd64.2.6.1.deb


sudo dpkg -i gcm-linux_amd64.2.6.1.deb
popd 
echo "✅ Git Credential Manager installed successfully."

git-credential-manager configure

echo "✅ Git Credential Manager configured successfully."

sudo apt update
sudo apt install -y gnupg pass
echo "✅ gpg and pass installed successfully."

git config --global credential.credentialStore gpg

echo "✅ Git configured to use GPG for credentials."

echo 'export GPG_TTY=$(tty)' >> ~/.profile

echo "✅ Added 'export GPG_TTY=\$(tty)' to ~/.profile."

read -s -p "Enter a passphrase for your new GPG key: " GPG_PASSPHRASE
echo
gpg --batch --passphrase "$GPG_PASSPHRASE" --quick-gen-key "$(whoami)@$(hostname)" default default never
GPG_KEY_ID=$(gpg --list-secret-keys --keyid-format=long "$(whoami)@$(hostname)" | grep 'sec' | awk '{print $2}' | cut -d'/' -f2 | head -n1)
echo "✅ Generated new GPG key. Key ID: $GPG_KEY_ID"

pass init "$GPG_KEY_ID"
echo "✅ pass initialized with GPG key $GPG_KEY_ID."

