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

