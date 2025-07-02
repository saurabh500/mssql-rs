#!/bin/bash

# Setup script to install git hooks for the project

echo "🔧 Setting up git hooks..."

# Get the project root directory (parent of the dev directory)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Check if we're in a git repository
if [ ! -d ".git" ]; then
    echo "❌ Error: Not in a git repository. Please run this from the project root or ensure .git exists"
    exit 1
fi

# Check if dev/hooks directory exists
if [ ! -d "dev/hooks" ]; then
    echo "❌ Error: dev/hooks directory not found in the repository"
    exit 1
fi

# Create .git/hooks directory if it doesn't exist
mkdir -p .git/hooks

# Copy and install pre-commit hook
if [ -f "dev/hooks/pre-commit" ]; then
    cp dev/hooks/pre-commit .git/hooks/pre-commit
    chmod +x .git/hooks/pre-commit
    echo "✅ pre-commit hook installed"
else
    echo "⚠️  Warning: dev/hooks/pre-commit not found"
fi

echo "🎉 Git hooks setup complete!"
echo ""
echo "The pre-commit hook will now:"
echo "  - Run 'cargo fmt' before each commit"
echo "  - Prevent commits if code formatting changes are needed"
echo ""
echo "To disable the hook temporarily, use: git commit --no-verify"
