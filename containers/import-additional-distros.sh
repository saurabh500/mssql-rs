#!/bin/bash
set -e

# Script to import additional distro images into Azure Container Registry
# This avoids rate limiting during CI builds
# Uses 'az acr import' which supports cross-architecture imports without local Docker

ACR_NAME="tdslibrs"
ACR_REGISTRY="tdslibrs.azurecr.io"

echo "Importing additional distro images to Azure Container Registry..."
echo "Registry: $ACR_REGISTRY"
echo ""

# Import Debian
echo "==> Importing Debian Bookworm..."
az acr import \
  --name "$ACR_NAME" \
  --source docker.io/library/debian:bookworm \
  --image import/debian:bookworm \
  --force

# Import Red Hat UBI9
echo "==> Importing Red Hat UBI9..."
az acr import \
  --name "$ACR_NAME" \
  --source docker.io/redhat/ubi9:latest \
  --image import/redhat/ubi9:latest \
  --force

# Import Oracle Linux 9
echo "==> Importing Oracle Linux 9..."
az acr import \
  --name "$ACR_NAME" \
  --source docker.io/library/oraclelinux:9 \
  --image import/oraclelinux:9 \
  --force

echo ""
echo "✅ Additional distro images imported successfully"
echo ""
echo "Imported images:"
echo "  - $ACR_REGISTRY/import/debian:bookworm"
echo "  - $ACR_REGISTRY/import/redhat/ubi9:latest"
echo "  - $ACR_REGISTRY/import/oraclelinux:9"
