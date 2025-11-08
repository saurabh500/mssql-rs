# Fuzzing Package for Remote Machines

This package contains everything needed to run fuzz testing on a different machine.

## 📦 Package Contents

- **`corpus-fuzz_token_stream.tar.gz`** (6KB) - Minimized corpus with 135 test cases
- **`run-fuzz.sh`** - Automated setup and execution script
- **`FUZZING-README.md`** - Complete documentation

## 🚀 Quick Start (3 steps)

1. **Copy the repository to your target machine:**
   ```bash
   # Copy the entire mssql-tds directory
   scp -r /path/to/mssql-tds user@remote-machine:/path/to/
   ```

2. **SSH into the target machine and navigate:**
   ```bash
   ssh user@remote-machine
   cd /path/to/mssql-tds/mssql-tds
   ```

3. **Run the fuzzing script:**
   ```bash
   ./fuzz/run-fuzz.sh 300  # Run for 5 minutes
   ```

That's it! The script will:
- ✅ Install Rust nightly toolchain (if needed)
- ✅ Install cargo-fuzz (if needed)
- ✅ Extract the corpus automatically
- ✅ Run fuzzing and report results

## 📊 What to Expect

**Normal run (no crashes):**
```
=== MSSQL-TDS Fuzz Testing Setup ===
✓ Nightly toolchain already installed
✓ cargo-fuzz already installed
✓ Extracted 135 corpus files

Running fuzzer...
  Target: fuzz_token_stream
  Duration: 300s
  
[Fuzzing output...]

=== Fuzzing Complete ===
Final corpus: 245 files (892K)
✓ No crashes found
```

**If crashes are found:**
```
⚠ Found 2 crash(es) in artifacts/fuzz_token_stream/
Review these crashes and document them in ../fuzz-bugs-found.md
```

## 🔧 Prerequisites

The script will install these automatically, but you can also install manually:

- **Rust** (if not already installed): https://rustup.rs/
- **Build essentials** (usually already present):
  ```bash
  # Ubuntu/Debian
  sudo apt-get install build-essential
  
  # CentOS/RHEL
  sudo yum groupinstall "Development Tools"
  ```

## 📝 Different Usage Scenarios

### Short CI validation (30 seconds)
```bash
./fuzz/run-fuzz.sh 30
```

### Standard testing (5 minutes)
```bash
./fuzz/run-fuzz.sh 300
```

### Overnight fuzzing (8 hours)
```bash
./fuzz/run-fuzz.sh 28800
```

### Continuous fuzzing (24 hours)
```bash
nohup ./fuzz/run-fuzz.sh 86400 > fuzz.log 2>&1 &
```

## 📂 File Locations

After running, you'll find:
- **Corpus:** `fuzz/corpus/fuzz_token_stream/` (extracted automatically)
- **Crashes:** `fuzz/artifacts/fuzz_token_stream/crash-*` (if any found)
- **Coverage:** Printed in the terminal output

## 🐛 If You Find Bugs

1. Crash files are in `fuzz/artifacts/fuzz_token_stream/`
2. Document in `fuzz-bugs-found.md` (follow existing format)
3. The crash file itself is the reproduction test case

## 🔍 Advanced Options

For more control, see `FUZZING-README.md` for:
- Corpus minimization
- Manual setup
- CI/CD integration
- Reproducing crashes
- Custom fuzzer options

## 📊 Corpus Statistics

**Initial corpus (included in archive):**
- Files: 135
- Uncompressed: 552KB
- Compressed: 6KB
- Coverage edges: 2,366
- Features: 3,421

This corpus was generated from 6,985 fuzzing executions and minimized to the smallest set that achieves maximum coverage.

## ⚙️ Script Behavior

The `run-fuzz.sh` script is idempotent and safe to run multiple times:
- Won't reinstall toolchains if already present
- Won't re-extract corpus if already extracted
- Safe to interrupt (Ctrl+C) and resume

## 🔗 Related Files

- **Bug tracker:** `../fuzz-bugs-found.md` (21 bugs found and fixed)
- **Fuzz targets:** `fuzz/fuzz_targets/*.rs`
- **Configuration:** `fuzz/Cargo.toml`

---

**Need help?** See `FUZZING-README.md` for detailed documentation.
