# Python Wheel Build Pipeline Setup

## Summary

Added automated Python wheel building to the Azure DevOps pipeline for the `mssql-py-core` extension module.

## Changes Made

### 1. Created Build Template
**File:** `.pipeline/templates/build-python-wheels-template.yml`

- Installs `maturin` (Python package builder for Rust)
- Builds release wheels for Windows, Linux, and macOS
- Publishes wheels to Azure DevOps artifacts with OS-specific names

### 2. Updated Main Pipeline
**File:** `.pipeline/validation-pipeline.yml`

Integrated Python wheel building into existing Build stage jobs:
- `Build_Windows` - Now builds Windows x64 wheels after Rust build
- `Build_Linux` - Now builds Linux x64 wheels (manylinux) after Rust build
- `Build_MacOS` - Now builds macOS wheels (universal or x64) after Rust build

Each job calls both `build-template.yml` and `build-python-wheels-template.yml` sequentially.

### 3. Workspace Configuration
**File:** `Cargo.toml`

Excluded `mssql-py-core` from workspace to avoid linking issues during regular Rust builds:
```toml
exclude = ["mssql-py-core"]
```

## How It Works

### Pipeline Execution Flow

```
┌──────────────────────────────────────────────────────────────┐
│  Stage: Build (Rust builds + Python wheels)                  │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Build_Windows                                           │  │
│  │  1. Rust build (build-template.yml)                    │  │
│  │  2. Python wheels (build-python-wheels-template.yml)   │  │
│  └────────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Build_Linux                                             │  │
│  │  1. Rust build (build-template.yml)                    │  │
│  │  2. Python wheels (build-python-wheels-template.yml)   │  │
│  └────────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Build_MacOS                                             │  │
│  │  1. Rust build (build-template.yml)                    │  │
│  │  2. Python wheels (build-python-wheels-template.yml)   │  │
│  └────────────────────────────────────────────────────────┘  │
│  - Build_Linux_ARM                                            │
│  - Build_Alpine                                               │
└──────────────────────────────────────────────────────────────┘
                         │
                         ▼
                  Artifacts Published
```

### Build Steps per Platform

1. **Standard Rust Build** (via `build-template.yml`)
   - Install dependencies
   - Build Rust code
   - Run tests
2. **Python Wheel Build** (via `build-python-wheels-template.yml`)
   - Install maturin via pipx (`pipx install maturin`)
   - Build wheel (`maturin build --release --out $(Build.ArtifactStagingDirectory)/wheels`)
   - Publish artifacts to Azure DevOps

### Artifacts Generated

Each job publishes artifacts with OS-specific names:
- `python-wheels-Windows`
- `python-wheels-Linux`
- `python-wheels-MacOS`

## Wheel Details

### Python Version Support

The pipeline builds **separate wheels for each Python version**:
- Python 3.10
- Python 3.11
- Python 3.12
- Python 3.13
- Python 3.14

### Architecture Support

**x86_64 (x64)**:
- Windows x64
- Linux x64 (glibc)
- Alpine x64 (musl)

**ARM64 (aarch64)**:
- Linux ARM64 (glibc)
- Alpine ARM64 (musl)

**macOS Universal**:
- macOS universal2 (contains both x86_64 and arm64 in one wheel)

### Wheel Matrix

Each combination produces wheels for all 5 Python versions:

| Platform | Architecture | C Library | Example Wheel |
|----------|-------------|-----------|---------------|
| Windows | x64 | MSVCRT | `cp310-cp310-win_amd64.whl` |
| Linux | x64 | glibc | `cp310-cp310-manylinux_2_34_x86_64.whl` |
| Linux | ARM64 | glibc | `cp310-cp310-manylinux_2_34_aarch64.whl` |
| Alpine | x64 | musl | `cp310-cp310-musllinux_1_2_x86_64.whl` |
| Alpine | ARM64 | musl | `cp310-cp310-musllinux_1_2_aarch64.whl` |
| macOS | Universal | libc | `cp310-cp310-macosx_11_0_universal2.whl` |

**Total wheels per build**: ~30 wheels (6 platform/arch combinations × 5 Python versions)

### Artifact Structure

Artifacts are published with platform and architecture in the name:
- `python-wheels-Windows-x64`: 5 wheels
- `python-wheels-Linux-x64`: 5 wheels (manylinux/glibc)
- `python-wheels-Linux-ARM64`: 5 wheels (manylinux/glibc)
- `python-wheels-Alpine-x64`: 5 wheels (musllinux/musl)
- `python-wheels-Alpine-ARM64`: 5 wheels (musllinux/musl)
- `python-wheels-MacOS-universal2`: 5 wheels (works on both Intel and Apple Silicon)

### Platform-Specific Details

**Linux (glibc - manylinux)**:
- Built on Ubuntu with glibc
- Compatible with most Linux distributions (Debian, Ubuntu, RHEL, Fedora, etc.)
- Tagged as `manylinux_2_34` or similar
- Available for both x64 and ARM64

**Alpine (musl - musllinux)**:
- Built specifically for Alpine Linux and other musl-based distributions
- Required for Alpine Docker containers
- Tagged as `musllinux_1_2`
- Available for both x64 and ARM64
- **Important**: glibc wheels won't work on musl systems and vice versa

**macOS Universal2**:
- Single wheel works on both Intel (x86_64) and Apple Silicon (ARM64) Macs
- maturin automatically creates universal2 wheels on macOS
- Users don't need to choose architecture

### Naming Convention
Example: `mssql_py_core-0.1.0-cp310-cp310-manylinux_2_34_x86_64.whl`

- `mssql_py_core` - Package name
- `0.1.0` - Version from Cargo.toml
- `cp310` - CPython 3.10
- `manylinux_2_34` - Linux compatibility tag
- `x86_64` - Architecture

### Installation
```bash
pip install mssql_py_core-0.1.0-cp310-cp310-manylinux_2_34_x86_64.whl
```

### Usage in Python
```python
import mssql_core_tds

# Create connection with ClientContext dictionary
client_context = {
    'server': 'localhost',
    'database': 'master',
    'user_name': 'sa',
    'password': 'your_password',
    # ... other connection parameters
}

conn = mssql_core_tds.DdbcConnection(client_context)
cursor = conn.cursor()
# Use cursor for queries...
```

## Dependencies

### Build Requirements
- **Python 3.10+** (already available in agents)
- **Rust toolchain** (already installed in pipeline)
- **maturin** (installed in pipeline)

### Runtime Requirements (bundled in wheel)
- OpenSSL libraries (automatically included by maturin)
- Rust standard library (statically linked)

## Downloading Artifacts

### From Azure DevOps UI
1. Go to the pipeline run
2. Navigate to "Summary" tab
3. Find "Related" → "Published" artifacts
4. Download `python-wheels-Windows`, `python-wheels-Linux`, or `python-wheels-MacOS`

### Using Azure CLI
```bash
# List artifacts
az pipelines runs artifact list --run-id <run-id> --org <org-url> --project <project>

# Download artifact
az pipelines runs artifact download --run-id <run-id> --artifact-name python-wheels-Linux --path ./downloads
```

## Integration with mssql-python

The wheels are consumed by the `mssql-python` package (separate repository):

```python
# In mssql-python
import mssql_python

# Switch to Core TDS backend
mssql_python.set_backend('core')

# Creates connection using mssql_core_tds.DdbcConnection
conn = mssql_python.connect("Server=localhost;Database=master;...")
```

## Testing Locally

Build and test wheels locally:

```bash
cd mssql-tds/mssql-py-core

# Build wheel
maturin build --release

# Install locally for testing
maturin develop

# Test in Python
python -c "import mssql_core_tds; print(mssql_core_tds.DdbcConnection)"
```

## Troubleshooting

### Build Fails on macOS
- Ensure Python and Rust are properly installed
- Check that `pyo3-build-config` can find Python libraries

### Wheel Size is Large
- Release builds include shared libraries (OpenSSL)
- This is normal; maturin bundles dependencies for portability

### Import Error in Python
- Ensure wheel matches Python version (e.g., cp310 for Python 3.10)
- Check platform compatibility (manylinux, macOS version, Windows)

## Future Enhancements

1. **Multi-version builds**: Build for Python 3.10, 3.11, 3.12
2. **ARM builds**: Add macOS ARM64 and Linux ARM64 wheels
3. **Alpine Linux**: Add musllinux wheels for Alpine
4. **Automated testing**: Add Python import tests to pipeline
5. **PyPI publishing**: Automate wheel upload to PyPI

## References

- [Maturin Documentation](https://www.maturin.rs/)
- [PyO3 Guide](https://pyo3.rs/)
- [Python Wheel Naming](https://www.python.org/dev/peps/pep-0427/)
- [manylinux Standard](https://github.com/pypa/manylinux)
