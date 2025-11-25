#!/bin/bash
set -e

# Build pre-configured Python wheel build images with Rust and maturin pre-installed
# These images extend the official PyPA manylinux/musllinux images

ACR_REGISTRY="tdslibrs.azurecr.io"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "======================================================"
echo "Building Python wheel build images with Rust pre-installed"
echo "======================================================"
echo ""

cd "$SCRIPT_DIR"

# Build manylinux x64 image
echo "==> Building manylinux x64 image..."
docker build \
    -f Dockerfile.PythonBuild.manylinux.x64 \
    -t "${ACR_REGISTRY}/python-build/manylinux_2_28_x86_64_rust:latest" \
    .
echo "✅ manylinux x64 image built successfully"
echo ""

# Build manylinux ARM64 image
echo "==> Building manylinux ARM64 image..."
docker build \
    -f Dockerfile.PythonBuild.manylinux.arm64 \
    -t "${ACR_REGISTRY}/python-build/manylinux_2_28_aarch64_rust:latest" \
    .
echo "✅ manylinux ARM64 image built successfully"
echo ""

# Build musllinux x64 image
echo "==> Building musllinux x64 image..."
docker build \
    -f Dockerfile.PythonBuild.musllinux.x64 \
    -t "${ACR_REGISTRY}/python-build/musllinux_1_2_x86_64_rust:latest" \
    .
echo "✅ musllinux x64 image built successfully"
echo ""

# Build musllinux ARM64 image
echo "==> Building musllinux ARM64 image..."
docker build \
    -f Dockerfile.PythonBuild.musllinux.arm64 \
    -t "${ACR_REGISTRY}/python-build/musllinux_1_2_aarch64_rust:latest" \
    .
echo "✅ musllinux ARM64 image built successfully"
echo ""

echo "======================================================"
echo "✅ All images built successfully!"
echo "======================================================"
echo ""
echo "Built images:"
echo "  - ${ACR_REGISTRY}/python-build/manylinux_2_28_x86_64_rust:latest"
echo "  - ${ACR_REGISTRY}/python-build/manylinux_2_28_aarch64_rust:latest"
echo "  - ${ACR_REGISTRY}/python-build/musllinux_1_2_x86_64_rust:latest"
echo "  - ${ACR_REGISTRY}/python-build/musllinux_1_2_aarch64_rust:latest"
echo ""
echo "To push to ACR, run:"
echo "  ./push-python-images.sh"
