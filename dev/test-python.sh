#!/bin/bash
set -e

# Script to run Python tests for mssql-py-core
# This script sets up a Python virtual environment, installs dependencies, and runs pytest
# Usage: test-python.sh [--skip-integration]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PY_CORE_DIR="$PROJECT_ROOT/mssql-py-core"
MOCK_TDS_PY_DIR="$PROJECT_ROOT/mssql-mock-tds-py"
VENV_DIR="$PROJECT_ROOT/.venv-pycore"

# Parse command line arguments
SKIP_INTEGRATION=false
if [[ "$1" == "--skip-integration" ]]; then
    SKIP_INTEGRATION=true
fi

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

# Build and install mssql-mock-tds-py (mock TDS server Python bindings)
echo "Building mssql-mock-tds-py with maturin develop..."
cd "$MOCK_TDS_PY_DIR"
PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin develop

# Return to py-core directory for tests
cd "$PY_CORE_DIR"

# Run tests
echo ""
if [ "$SKIP_INTEGRATION" = true ]; then
    echo "Running pytest (unit tests only, skipping integration tests)..."
    echo "==================================="
    pytest tests/ -v -m "not integration" --junitxml=pytest-results.xml
else
    echo "Running pytest (unit and integration tests)..."
    echo "==================================="
    pytest tests/ -v --junitxml=pytest-results.xml
fi

echo ""
echo "==================================="
echo "Tests completed successfully!"
echo "==================================="
