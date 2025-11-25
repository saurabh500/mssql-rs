#!/bin/bash
set -e

# Build Python wheels inside manylinux/musllinux containers
# This script is designed to run inside the container

PYTHON_VERSIONS=("3.10" "3.11" "3.12" "3.13" "3.14")
WORKSPACE_DIR="${WORKSPACE_DIR:-/workspace}"
OUTPUT_DIR="${OUTPUT_DIR:-$WORKSPACE_DIR/target/wheels}"

echo "==> Building Python wheels in container"
echo "Workspace: $WORKSPACE_DIR"
echo "Output directory: $OUTPUT_DIR"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Find a Python binary for installing tools
FIRST_PYTHON=""
for py_path in /opt/python/cp312-cp312/bin/python /opt/python/cp3*/bin/python /usr/local/bin/python3 /usr/bin/python3; do
    if [ -x "$py_path" ]; then
        FIRST_PYTHON="$py_path"
        break
    fi
done

if [ -z "$FIRST_PYTHON" ]; then
    echo "Error: No Python installation found in container!"
    exit 1
fi

echo "Using Python: $FIRST_PYTHON"

# Install required system dependencies for building
echo ""
echo "==> Installing system dependencies..."
if command -v yum &> /dev/null; then
    # RHEL/CentOS based (manylinux)
    yum install -y openssl-devel pkgconfig || true
elif command -v apk &> /dev/null; then
    # Alpine based (musllinux)
    apk add --no-cache openssl-dev pkgconfig || true
fi

# Install Rust if not already installed
if ! command -v cargo &> /dev/null; then
    echo ""
    echo "==> Installing Rust toolchain..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    export PATH="$HOME/.cargo/bin:$PATH"
    echo "Rust installed successfully"
    rustc --version
    cargo --version
else
    echo "Rust already installed"
    export PATH="$HOME/.cargo/bin:$PATH"
fi

# Install maturin if not already installed
if ! $FIRST_PYTHON -m pip show maturin &> /dev/null; then
    echo ""
    echo "==> Installing maturin..."
    $FIRST_PYTHON -m pip install maturin
    echo "Maturin installed successfully"
fi

cd "$WORKSPACE_DIR/mssql-py-core"

# Build wheels for each Python version
for PY_VERSION in "${PYTHON_VERSIONS[@]}"; do
    # Find the Python binary (manylinux uses cpython naming)
    PYTHON_BIN=""
    
    # Try different naming conventions
    for py_path in /opt/python/cp${PY_VERSION//./}-*/bin/python /usr/local/bin/python${PY_VERSION} /usr/bin/python${PY_VERSION}; do
        if [ -x "$py_path" ]; then
            PYTHON_BIN="$py_path"
            break
        fi
    done
    
    if [ -z "$PYTHON_BIN" ]; then
        echo "⚠️  Python $PY_VERSION not found, skipping..."
        continue
    fi
    
    echo ""
    echo "==> Building wheel for Python $PY_VERSION using $PYTHON_BIN"
    $PYTHON_BIN --version
    
    $FIRST_PYTHON -m maturin build --release \
        --interpreter "$PYTHON_BIN" \
        --out "$OUTPUT_DIR" \
        --manifest-path "$WORKSPACE_DIR/mssql-py-core/Cargo.toml"
    
    echo "✅ Wheel built successfully for Python $PY_VERSION"
done

# For manylinux, run auditwheel to ensure compatibility
if command -v auditwheel &> /dev/null; then
    echo ""
    echo "==> Running auditwheel to verify manylinux compatibility..."
    for wheel in "$OUTPUT_DIR"/*.whl; do
        if [ -f "$wheel" ]; then
            echo "Checking: $(basename "$wheel")"
            auditwheel show "$wheel" || echo "⚠️  auditwheel check failed for $wheel"
        fi
    done
fi

echo ""
echo "==> All wheels built successfully!"
ls -lh "$OUTPUT_DIR"
