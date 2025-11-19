#!/bin/bash

echo "PATH variable:"
echo "$PATH"

echo ""
echo "Azure CLI information:"
if command -v az &> /dev/null; then
    az version
else
    echo "Azure CLI is not installed"
fi

echo ""
echo "gss-ntlmssp package information:"
if dpkg -l | grep -q gss-ntlmssp; then
    dpkg -s gss-ntlmssp | grep Version
    echo "Check vulnerability: https://ubuntu.com/security/notices/USN-7588-1"
    echo "Vulnerable version: 0.7.0-4build4"
    echo "Fixed version: 0.7.0-4ubuntu0.22.04.1~esm1"
else
    echo "gss-ntlmssp package is not installed"
fi

echo ""
echo "binutils packages information:"
for pkg in libbinutils binutils-common binutils-aarch64-linux-gnu libctf0 binutils libctf-nobfd0; do
    if dpkg -l | grep -q "^ii  $pkg "; then
        version=$(dpkg -s $pkg 2>/dev/null | grep "^Version:" | awk '{print $2}')
        echo "$pkg: $version"
    fi
done
echo "Vulnerable version: 2.38-4ubuntu2.8"
echo "Fixed version: 2.38-4ubuntu2.10"

