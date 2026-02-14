#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== MSSQL-TDS Fuzz Coverage Analysis ===${NC}"
echo ""

# Check if we're in the fuzz directory
if [ ! -f "Cargo.toml" ] || [ ! -d "corpus" ]; then
    echo -e "${RED}Error: This script must be run from the mssql-tds/fuzz directory${NC}"
    exit 1
fi

# Check if rustup is installed
if ! command -v rustup &> /dev/null; then
    echo -e "${RED}Error: rustup is not installed${NC}"
    exit 1
fi

# Check if nightly toolchain is installed
if ! rustup toolchain list | grep -q "nightly"; then
    echo -e "${YELLOW}Installing nightly toolchain...${NC}"
    rustup toolchain install nightly
else
    echo -e "${GREEN}✓ Nightly toolchain installed${NC}"
fi

# Check if cargo-fuzz is installed
if ! cargo +nightly fuzz --version &> /dev/null; then
    echo -e "${YELLOW}Installing cargo-fuzz...${NC}"
    cargo +nightly install cargo-fuzz
else
    echo -e "${GREEN}✓ cargo-fuzz installed${NC}"
fi

# List available fuzz targets
FUZZ_TARGETS=("fuzz_token_stream" "fuzz_tds_client")

echo ""
echo -e "${BLUE}=== Running Coverage Analysis ===${NC}"
echo ""

# Find "$LLVM_COV" and "$LLVM_PROFDATA" from nightly toolchain
LLVM_COV=$(find ~/.rustup/toolchains/nightly*/lib/rustlib -name "llvm-cov" 2>/dev/null | head -1)
LLVM_PROFDATA=$(find ~/.rustup/toolchains/nightly*/lib/rustlib -name "llvm-profdata" 2>/dev/null | head -1)

if [ -z "$LLVM_COV" ] || [ -z "$LLVM_PROFDATA" ]; then
    echo -e "${RED}Error: LLVM coverage tools not found${NC}"
    echo "Run: rustup component add llvm-tools --toolchain nightly"
    exit 1
fi

echo -e "${GREEN}Using LLVM tools from nightly toolchain${NC}"

# Create a directory for coverage reports
COVERAGE_DIR="coverage-reports"
mkdir -p "$COVERAGE_DIR"

# Process each fuzz target
for TARGET in "${FUZZ_TARGETS[@]}"; do
    echo -e "${YELLOW}Processing target: $TARGET${NC}"
    
    # Check if corpus exists
    CORPUS_DIR="corpus/$TARGET"
    if [ ! -d "$CORPUS_DIR" ] || [ -z "$(ls -A $CORPUS_DIR 2>/dev/null)" ]; then
        echo -e "${YELLOW}  ⚠ No corpus found for $TARGET, skipping coverage${NC}"
        echo ""
        continue
    fi
    
    CORPUS_COUNT=$(find "$CORPUS_DIR" -type f | wc -l)
    echo -e "  Corpus: $CORPUS_COUNT files"
    
    # Run coverage for this target
    echo -e "  Running coverage analysis..."
    cargo +nightly fuzz coverage "$TARGET" 2>&1 | grep -v "^Compiling\|^Finished\|^Running" || true
    
    # Check if coverage data was generated
    COV_DIR="coverage/$TARGET"
    if [ -d "$COV_DIR" ]; then
        echo -e "${GREEN}  ✓ Coverage data generated${NC}"
        
        # Generate coverage report using llvm-cov
        echo -e "  Generating HTML report..."
        
        # Find the binary
        BINARY=$(find "target/x86_64-unknown-linux-gnu/coverage" -name "$TARGET" -type f 2>/dev/null | head -1)
        
        if [ -n "$BINARY" ]; then
            # Generate per-target report
            "$LLVM_COV" show "$BINARY" \
                --format=html \
                --instr-profile="$COV_DIR/coverage.profdata" \
                --ignore-filename-regex="rustc|cargo|registry" \
                --output-dir="$COVERAGE_DIR/$TARGET" \
                2>/dev/null || echo -e "${YELLOW}  ⚠ Could not generate HTML report${NC}"
            
            if [ -d "$COVERAGE_DIR/$TARGET" ]; then
                echo -e "${GREEN}  ✓ HTML report: $COVERAGE_DIR/$TARGET/index.html${NC}"
            fi
            
            # Generate text summary
            "$LLVM_COV" report "$BINARY" \
                --instr-profile="$COV_DIR/coverage.profdata" \
                --ignore-filename-regex="rustc|cargo|registry" \
                > "$COVERAGE_DIR/$TARGET-summary.txt" \
                2>/dev/null || true
            
            if [ -f "$COVERAGE_DIR/$TARGET-summary.txt" ]; then
                echo -e "  Coverage summary:"
                head -20 "$COVERAGE_DIR/$TARGET-summary.txt" | sed 's/^/    /'
            fi
        else
            echo -e "${YELLOW}  ⚠ Could not find coverage binary${NC}"
        fi
    else
        echo -e "${YELLOW}  ⚠ No coverage data generated${NC}"
    fi
    
    echo ""
done

# Try to generate a combined report
echo -e "${BLUE}=== Generating Combined Coverage Report ===${NC}"
echo ""

# Find all coverage binaries
BINARIES=()
PROFDATA_FILES=()

for TARGET in "${FUZZ_TARGETS[@]}"; do
    BINARY=$(find "target/x86_64-unknown-linux-gnu/coverage/release" -name "$TARGET" -type f 2>/dev/null | head -1)
    COV_DIR="coverage/$TARGET"
    
    if [ -n "$BINARY" ] && [ -f "$COV_DIR/coverage.profdata" ]; then
        BINARIES+=("$BINARY")
        PROFDATA_FILES+=("$COV_DIR/coverage.profdata")
    fi
done

if [ ${#BINARIES[@]} -gt 0 ]; then
    echo -e "${YELLOW}Combining coverage from ${#BINARIES[@]} target(s)...${NC}"
    
    # Merge profdata files if we have more than one
    if [ ${#PROFDATA_FILES[@]} -gt 1 ]; then
        "$LLVM_PROFDATA" merge -sparse "${PROFDATA_FILES[@]}" -o "$COVERAGE_DIR/combined.profdata" 2>/dev/null || {
            echo -e "${YELLOW}Could not merge profdata files, using individual reports${NC}"
            PROFDATA_FILE="${PROFDATA_FILES[0]}"
        }
        PROFDATA_FILE="$COVERAGE_DIR/combined.profdata"
    else
        PROFDATA_FILE="${PROFDATA_FILES[0]}"
    fi
    
    # Build the object list for llvm-cov
    OBJECT_ARGS=()
    for i in "${!BINARIES[@]}"; do
        if [ $i -eq 0 ]; then
            OBJECT_ARGS+=("${BINARIES[$i]}")
        else
            OBJECT_ARGS+=("-object" "${BINARIES[$i]}")
        fi
    done
    
    # Generate combined HTML report
    "$LLVM_COV" show "${OBJECT_ARGS[@]}" \
        --format=html \
        --instr-profile="$PROFDATA_FILE" \
        --ignore-filename-regex="rustc|cargo|registry" \
        --output-dir="$COVERAGE_DIR/combined" \
        2>/dev/null || echo -e "${YELLOW}Could not generate combined HTML report${NC}"
    
    if [ -d "$COVERAGE_DIR/combined" ]; then
        echo -e "${GREEN}✓ Combined HTML report: $COVERAGE_DIR/combined/index.html${NC}"
    fi
    
    # Generate combined text summary
    "$LLVM_COV" report "${OBJECT_ARGS[@]}" \
        --instr-profile="$PROFDATA_FILE" \
        --ignore-filename-regex="rustc|cargo|registry" \
        > "$COVERAGE_DIR/combined-summary.txt" \
        2>/dev/null || true
    
    if [ -f "$COVERAGE_DIR/combined-summary.txt" ]; then
        echo -e ""
        echo -e "${GREEN}=== Combined Coverage Summary ===${NC}"
        head -30 "$COVERAGE_DIR/combined-summary.txt"
    fi
else
    echo -e "${YELLOW}No coverage data available to combine${NC}"
fi

echo ""
echo -e "${GREEN}=== Coverage Analysis Complete ===${NC}"
echo ""
echo "Coverage reports saved in: $COVERAGE_DIR/"
echo ""
echo "To view the combined HTML report:"
echo "  xdg-open $COVERAGE_DIR/combined/index.html"
echo ""
