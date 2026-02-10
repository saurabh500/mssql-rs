#!/bin/bash
# Apply CI-specific cargo configuration
# This script is called in CI pipelines to use authenticated ADO feeds

set -euo pipefail

echo "Applying CI cargo configuration..."
cp .cargo/config.ci.toml .cargo/config.toml
echo "CI cargo config applied. Using authenticated ADO feeds."
