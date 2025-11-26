#!/bin/bash
set -e

# Script to import official Alpine 3.18 base images into Azure Container Registry
# This avoids rate limiting during CI builds
# Uses 'az acr import' which supports cross-architecture imports without local Docker

ACR_NAME="tdslibrs"
ACR_REGISTRY="tdslibrs.azurecr.io"

echo "Importing Alpine 3.18 base images to Azure Container Registry..."
echo "Registry: $ACR_REGISTRY"
echo ""
echo "Note: Using 'az acr import' - works for all architectures without local Docker pull"
echo ""

# Alpine 3.18 multi-arch image (includes x64, arm64, and other architectures)
echo "==> Importing Alpine 3.18 multi-arch image..."
az acr import \
  --name "$ACR_NAME" \
  --source docker.io/library/alpine:3.18 \
  --image alpine:3.18 \
  --force

echo ""
echo "✅ Alpine base images imported successfully"
echo ""
echo "Imported images:"
echo "  - $ACR_REGISTRY/alpine:3.18 (multi-arch: x64, arm64, armv7, etc.)"
