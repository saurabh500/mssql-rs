#!/bin/bash
set -e

# Script to import official Alpine base images into Azure Container Registry
# This avoids rate limiting during CI builds
# Uses 'az acr import' which supports cross-architecture imports without local Docker

ACR_NAME="tdslibrs"
ACR_REGISTRY="tdslibrs.azurecr.io"

echo "Importing Alpine base images to Azure Container Registry..."
echo "Registry: $ACR_REGISTRY"
echo ""
echo "Note: Using 'az acr import' - works for all architectures without local Docker pull"
echo ""

# Alpine 3.18 multi-arch image
echo "==> Importing Alpine 3.18 multi-arch image..."
az acr import \
  --name "$ACR_NAME" \
  --source docker.io/library/alpine:3.18 \
  --image alpine:3.18 \
  --force

# Alpine 3.19 multi-arch image
echo "==> Importing Alpine 3.19 multi-arch image..."
az acr import \
  --name "$ACR_NAME" \
  --source docker.io/library/alpine:3.19 \
  --image alpine:3.19 \
  --force

# Alpine 3.20 multi-arch image
echo "==> Importing Alpine 3.20 multi-arch image..."
az acr import \
  --name "$ACR_NAME" \
  --source docker.io/library/alpine:3.20 \
  --image alpine:3.20 \
  --force

# Alpine 3.21 multi-arch image
echo "==> Importing Alpine 3.21 multi-arch image..."
az acr import \
  --name "$ACR_NAME" \
  --source docker.io/library/alpine:3.21 \
  --image alpine:3.21 \
  --force

echo ""
echo "✅ Alpine base images imported successfully"
echo ""
echo "Imported images:"
echo "  - $ACR_REGISTRY/alpine:3.18 (multi-arch)"
echo "  - $ACR_REGISTRY/alpine:3.19 (multi-arch)"
echo "  - $ACR_REGISTRY/alpine:3.20 (multi-arch)"
echo "  - $ACR_REGISTRY/alpine:3.21 (multi-arch)"
