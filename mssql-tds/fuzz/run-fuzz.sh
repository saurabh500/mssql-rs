#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== MSSQL-TDS Fuzz Testing Setup ===${NC}"
echo ""

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ] || [ ! -d "fuzz" ]; then
    echo -e "${RED}Error: This script must be run from the mssql-tds/mssql-tds directory${NC}"
    echo "Expected structure: mssql-tds/mssql-tds/fuzz/"
    exit 1
fi

# Check if rustup is installed
if ! command -v rustup &> /dev/null; then
    echo -e "${RED}Error: rustup is not installed${NC}"
    echo "Please install rustup from: https://rustup.rs/"
    exit 1
fi

echo -e "${YELLOW}Checking Rust toolchain...${NC}"

# Check if nightly toolchain is installed
if ! rustup toolchain list | grep -q "nightly"; then
    echo -e "${YELLOW}Installing nightly toolchain...${NC}"
    rustup toolchain install nightly
else
    echo -e "${GREEN}✓ Nightly toolchain already installed${NC}"
fi

# Check if cargo-fuzz is installed
if ! cargo +nightly fuzz --version &> /dev/null; then
    echo -e "${YELLOW}Installing cargo-fuzz...${NC}"
    cargo +nightly install cargo-fuzz
else
    echo -e "${GREEN}✓ cargo-fuzz already installed${NC}"
fi

# Extract corpus if archive exists and corpus directory is empty or missing
CORPUS_DIR="fuzz/corpus/fuzz_token_stream"
CORPUS_ARCHIVE="fuzz/corpus-fuzz_token_stream.tar.gz"

if [ -f "$CORPUS_ARCHIVE" ]; then
    if [ ! -d "$CORPUS_DIR" ] || [ -z "$(ls -A $CORPUS_DIR 2>/dev/null)" ]; then
        echo -e "${YELLOW}Extracting corpus from archive...${NC}"
        cd fuzz
        tar -xzf corpus-fuzz_token_stream.tar.gz
        cd ..
        
        FILE_COUNT=$(find $CORPUS_DIR -type f | wc -l)
        echo -e "${GREEN}✓ Extracted $FILE_COUNT corpus files${NC}"
    else
        echo -e "${GREEN}✓ Corpus already exists ($CORPUS_DIR)${NC}"
    fi
else
    echo -e "${YELLOW}Warning: Corpus archive not found at $CORPUS_ARCHIVE${NC}"
    echo "Fuzzing will start from an empty corpus"
fi

echo ""
echo -e "${GREEN}=== Setup Complete ===${NC}"
echo ""

# Parse command line arguments
FUZZ_TIME=${1:-60}
FUZZ_TARGET=${2:-fuzz_token_stream}

echo -e "${YELLOW}Running fuzzer...${NC}"
echo "  Target: $FUZZ_TARGET"
echo "  Duration: ${FUZZ_TIME}s"
echo "  Timeout per input: 10s"
echo ""

# Run the fuzzer
cd fuzz
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run $FUZZ_TARGET -- \
    -max_total_time=$FUZZ_TIME \
    -timeout=10

echo ""
echo -e "${GREEN}=== Fuzzing Complete ===${NC}"
echo ""

# Show statistics
if [ -d "corpus/$FUZZ_TARGET" ]; then
    CORPUS_COUNT=$(find corpus/$FUZZ_TARGET -type f | wc -l)
    CORPUS_SIZE=$(du -sh corpus/$FUZZ_TARGET | cut -f1)
    echo "Final corpus: $CORPUS_COUNT files ($CORPUS_SIZE)"
fi

if [ -d "artifacts/$FUZZ_TARGET" ]; then
    CRASH_COUNT=$(find artifacts/$FUZZ_TARGET -type f -name "crash-*" 2>/dev/null | wc -l)
    if [ $CRASH_COUNT -gt 0 ]; then
        echo -e "${RED}⚠ Found $CRASH_COUNT crash(es) in artifacts/$FUZZ_TARGET/${NC}"
        echo "Review these crashes and document them in ../fuzz-bugs-found.md"
    else
        echo -e "${GREEN}✓ No crashes found${NC}"
    fi
fi

echo ""
echo "To minimize the corpus, run:"
echo "  cd $(pwd)"
echo "  RUSTFLAGS=\"--cfg fuzzing\" cargo +nightly fuzz cmin $FUZZ_TARGET"
echo ""
echo "To continue fuzzing for longer, run:"
echo "  cd $(pwd)"
echo "  RUSTFLAGS=\"--cfg fuzzing\" cargo +nightly fuzz run $FUZZ_TARGET -- -max_total_time=300"
