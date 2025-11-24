#!/bin/bash
set -e

# Script to import official Python wheel building images into Azure Container Registry
# This avoids rate limiting during CI builds
# Uses 'az acr import' which supports cross-architecture imports without local Docker

ACR_NAME="tdslibrs"
ACR_REGISTRY="tdslibrs.azurecr.io"

echo "Importing Python wheel build images to Azure Container Registry..."
echo "Registry: $ACR_REGISTRY"
echo ""
echo "Note: Using 'az acr import' - works for all architectures without local Docker pull"
echo ""

# manylinux images for glibc-based systems (Ubuntu, RHEL, etc.)
echo "==> Importing manylinux x64 image..."
az acr import \
  --name "$ACR_NAME" \
  --source quay.io/pypa/manylinux_2_28_x86_64:latest \
  --image python-build/manylinux_2_28_x86_64:latest \
  --force

echo ""
echo "==> Importing manylinux arm64 image..."
az acr import \
  --name "$ACR_NAME" \
  --source quay.io/pypa/manylinux_2_28_aarch64:latest \
  --image python-build/manylinux_2_28_aarch64:latest \
  --force

# musllinux images for musl-based systems (Alpine)
echo ""
echo "==> Importing musllinux x64 image..."
az acr import \
  --name "$ACR_NAME" \
  --source quay.io/pypa/musllinux_1_2_x86_64:latest \
  --image python-build/musllinux_1_2_x86_64:latest \
  --force

echo ""
echo "==> Importing musllinux arm64 image..."
az acr import \
  --name "$ACR_NAME" \
  --source quay.io/pypa/musllinux_1_2_aarch64:latest \
  --image python-build/musllinux_1_2_aarch64:latest \
  --force

echo ""
echo "✅ All Python build images imported successfully!"
echo ""
echo "Images available in ACR:"
echo "  - ${ACR_REGISTRY}/python-build/manylinux_2_28_x86_64:latest"
echo "  - ${ACR_REGISTRY}/python-build/manylinux_2_28_aarch64:latest"
echo "  - ${ACR_REGISTRY}/python-build/musllinux_1_2_x86_64:latest"
echo "  - ${ACR_REGISTRY}/python-build/musllinux_1_2_aarch64:latest"
echo "  - ${ACR_NAME}/python-build/musllinux_1_2_aarch64:latest"
