#!/bin/bash
set -e

# Script to run Python tests for mssql-py-core
# This script sets up a Python virtual environment, installs dependencies, and runs pytest

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PY_CORE_DIR="$PROJECT_ROOT/mssql-py-core"
VENV_DIR="$PROJECT_ROOT/.venv-pycore"

echo "==================================="
echo "Python Test Runner for mssql-py-core"
echo "==================================="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating Python virtual environment at $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
    echo "Virtual environment created."
fi

# Activate virtual environment
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip -q

# Install dependencies
echo "Installing dependencies (maturin, pytest, pytest-asyncio, python-dotenv)..."
pip install maturin pytest pytest-asyncio python-dotenv -q

# Navigate to mssql-py-core directory
cd "$PY_CORE_DIR"

# Build and install the module in development mode
echo "Building mssql-py-core with maturin develop..."
maturin develop

# Run tests
echo ""
echo "Running pytest (excluding integration tests by default)..."
echo "==================================="
pytest tests/ -v -m "not integration"

echo ""
echo "==================================="
echo "Tests completed successfully!"
echo "To run integration tests: pytest tests/ -v -m integration"
echo "==================================="
