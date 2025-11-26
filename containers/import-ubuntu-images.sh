#!/bin/bash
set -e

# Script to import official Ubuntu 22.04 base images into Azure Container Registry
# This avoids rate limiting during CI builds
# Uses 'az acr import' which supports cross-architecture imports without local Docker

ACR_NAME="tdslibrs"
ACR_REGISTRY="tdslibrs.azurecr.io"

echo "Importing Ubuntu 22.04 base images to Azure Container Registry..."
echo "Registry: $ACR_REGISTRY"
echo ""
echo "Note: Using 'az acr import' - works for all architectures without local Docker pull"
echo ""

# Ubuntu 22.04 x64 image
echo "==> Importing Ubuntu 22.04 x64 image..."
az acr import \
  --name "$ACR_NAME" \
  --source docker.io/library/ubuntu:22.04 \
  --image ubuntu:22.04 \
  --force

# Ubuntu 22.04 arm64 image
echo ""
echo "==> Importing Ubuntu 22.04 arm64 image..."
az acr import \
  --name "$ACR_NAME" \
  --source docker.io/arm64v8/ubuntu:22.04 \
  --image arm64/ubuntu:22.04 \
  --force

echo ""
echo "✅ Ubuntu base images imported successfully"
echo ""
echo "Imported images:"
echo "  - $ACR_REGISTRY/ubuntu:22.04 (x64)"
echo "  - $ACR_REGISTRY/arm64/ubuntu:22.04 (arm64)"
