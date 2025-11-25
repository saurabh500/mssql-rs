#!/bin/bash
set -e

# Local testing script for Python wheel building
# Usage: ./test-wheel-build-local.sh [x64|arm64] [linux|alpine]

ARCH="${1:-x64}"
OS_TYPE="${2:-linux}"

echo "==> Testing Python wheel build locally"
echo "Architecture: $ARCH"
echo "OS Type: $OS_TYPE"

# Determine container image
if [ "$OS_TYPE" = "linux" ]; then
    if [ "$ARCH" = "x64" ]; then
        CONTAINER_IMAGE="tdslibrs.azurecr.io/python-build/manylinux_2_28_x86_64:latest"
        SHELL_CMD="bash"
    elif [ "$ARCH" = "arm64" ]; then
        CONTAINER_IMAGE="tdslibrs.azurecr.io/python-build/manylinux_2_28_aarch64:latest"
        SHELL_CMD="bash"
    else
        echo "Error: Unsupported architecture: $ARCH"
        exit 1
    fi
elif [ "$OS_TYPE" = "alpine" ]; then
    if [ "$ARCH" = "x64" ]; then
        CONTAINER_IMAGE="tdslibrs.azurecr.io/python-build/musllinux_1_2_x86_64:latest"
        SHELL_CMD="sh"
    elif [ "$ARCH" = "arm64" ]; then
        CONTAINER_IMAGE="tdslibrs.azurecr.io/python-build/musllinux_1_2_aarch64:latest"
        SHELL_CMD="sh"
    else
        echo "Error: Unsupported architecture: $ARCH"
        exit 1
    fi
else
    echo "Error: Unsupported OS type: $OS_TYPE (use 'linux' or 'alpine')"
    exit 1
fi

echo "Container image: $CONTAINER_IMAGE"

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Create output directory
OUTPUT_DIR="$PROJECT_ROOT/target/wheels"
mkdir -p "$OUTPUT_DIR"

echo ""
echo "==> Logging into ACR..."
az acr login -n tdslibrs

echo ""
echo "==> Pulling container image..."
docker pull "$CONTAINER_IMAGE"

echo ""
echo "==> Building wheels in container..."
docker run --rm \
    -v "$PROJECT_ROOT:/workspace" \
    -v "$OUTPUT_DIR:/workspace/target/wheels" \
    -e "WORKSPACE_DIR=/workspace" \
    -e "OUTPUT_DIR=/workspace/target/wheels" \
    "$CONTAINER_IMAGE" \
    $SHELL_CMD /workspace/scripts/build-python-wheels-in-container.sh

echo ""
echo "==> ✅ Wheels built successfully!"
echo "Output directory: $OUTPUT_DIR"
echo ""
ls -lh "$OUTPUT_DIR"

echo ""
echo "==> You can test the wheels with:"
echo "    pip install $OUTPUT_DIR/<wheel-name>.whl"
