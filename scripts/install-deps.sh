#!/bin/bash

pushd /tmp  
wget -q https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt update
sudo apt install azureauth

sudo apt install jq unzip build-essential pkg-config libssl-dev -y

popd 

