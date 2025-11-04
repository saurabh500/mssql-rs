#!/bin/bash
# Script to convert markdown presentation to PowerPoint using pandoc
# Usage: ./convert-to-pptx.sh [markdown-file] [output-file]
#
# If no arguments provided, converts fuzz-testing-presentation.md by default

set -e

# Default files
DEFAULT_INPUT="fuzz-testing-presentation.md"
DEFAULT_OUTPUT="fuzz-testing-presentation.pptx"

# Use provided arguments or defaults
INPUT_FILE="${1:-$DEFAULT_INPUT}"
OUTPUT_FILE="${2:-$DEFAULT_OUTPUT}"

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Change to script directory
cd "$SCRIPT_DIR"

# Check if pandoc is installed
if ! command -v pandoc &> /dev/null; then
    echo "Error: pandoc is not installed"
    echo "Please install pandoc:"
    echo "  Ubuntu/Debian: sudo apt-get install pandoc"
    echo "  macOS: brew install pandoc"
    exit 1
fi

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file '$INPUT_FILE' not found"
    exit 1
fi

echo "Converting $INPUT_FILE to PowerPoint..."

# Convert markdown to PowerPoint
pandoc "$INPUT_FILE" \
    -o "$OUTPUT_FILE" \
    -t pptx \
    --slide-level=2

if [ $? -eq 0 ]; then
    echo "Success! Created: $OUTPUT_FILE"
    
    # Get file size
    SIZE=$(ls -lh "$OUTPUT_FILE" | awk '{print $5}')
    echo "File size: $SIZE"
    
    # Optional: Copy to Windows Documents folder if in WSL
    if grep -qi microsoft /proc/version 2>/dev/null; then
        WINDOWS_USER=$(whoami)
        WINDOWS_DOCS="/mnt/c/Users/${WINDOWS_USER}/Documents"
        
        if [ -d "$WINDOWS_DOCS" ]; then
            read -p "Copy to Windows Documents folder? (y/n) " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                cp "$OUTPUT_FILE" "$WINDOWS_DOCS/"
                echo "Copied to: $WINDOWS_DOCS/$OUTPUT_FILE"
            fi
        fi
    fi
else
    echo "Error: Conversion failed"
    exit 1
fi
