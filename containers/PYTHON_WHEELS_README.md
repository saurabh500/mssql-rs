# Python Wheel Building with Containers

This directory contains scripts and Dockerfiles for building Python wheels in a consistent, reproducible manner.

## Overview

We build Python wheels inside **manylinux** and **musllinux** containers to:
- ✅ Avoid GitHub API rate limits (no need to download Python from GitHub)
- ✅ Ensure binary compatibility across Linux distributions
- ✅ Use official PyPA (Python Packaging Authority) build environments
- ✅ Build for multiple Python versions (3.10-3.14) in one step
- ✅ Support both x64 and ARM64 architectures

## Container Images

All images are hosted in your Azure Container Registry (`tdslibrs.azurecr.io`) to avoid Docker Hub rate limits.

### Linux (glibc-based - Ubuntu, RHEL, Debian, etc.)
- **x64**: `tdslibrs.azurecr.io/python-build/manylinux_2_28_x86_64:latest`
- **ARM64**: `tdslibrs.azurecr.io/python-build/manylinux_2_28_aarch64:latest`

### Alpine (musl-based)
- **x64**: `tdslibrs.azurecr.io/python-build/musllinux_1_2_x86_64:latest`
- **ARM64**: `tdslibrs.azurecr.io/python-build/musllinux_1_2_aarch64:latest`

### MacOS
- Built natively on MacOS agents (no container)

### Windows
- ❌ **Not supported** - Windows wheel builds are disabled

## Setup Instructions

### 1. Import Container Images to ACR

Run this **once** to pull official PyPA images and push them to your ACR:

```bash
cd containers
chmod +x import-python-build-images.sh
./import-python-build-images.sh
```

This script:
1. Logs into Azure Container Registry
2. Pulls official images from `quay.io/pypa`
3. Tags them for your ACR
4. Pushes them to `tdslibrs.azurecr.io`

**Note**: These are official, maintained images. You should periodically re-run this script to get updates (monthly is recommended).

### 2. Build Wheels Locally (Optional)

To test wheel building locally:

```bash
# For x64 Linux (manylinux)
docker run --rm \
  -v "$(pwd):/workspace" \
  -v "$(pwd)/target/wheels:/workspace/target/wheels" \
  tdslibrs.azurecr.io/python-build/manylinux_2_28_x86_64:latest \
  bash /workspace/scripts/build-python-wheels-in-container.sh

# For x64 Alpine (musllinux)
docker run --rm \
  -v "$(pwd):/workspace" \
  -v "$(pwd)/target/wheels:/workspace/target/wheels" \
  tdslibrs.azurecr.io/python-build/musllinux_1_2_x86_64:latest \
  sh /workspace/scripts/build-python-wheels-in-container.sh
```

Wheels will be created in `target/wheels/`.

## Pipeline Integration

The Azure DevOps pipeline automatically builds wheels in containers:

**Template**: `.pipeline/templates/build-python-wheels-template.yml`

**Usage**:
```yaml
- template: templates/build-python-wheels-template.yml
  parameters:
    osType: Linux        # Linux, Alpine, or MacOS
    architecture: x64    # x64 or ARM64
```

**What it does**:
1. Logs into ACR
2. Pulls the appropriate container image
3. Mounts source code into the container
4. Runs `build-python-wheels-in-container.sh`
5. Builds wheels for Python 3.10, 3.11, 3.12, 3.13, 3.14
6. Publishes wheels as build artifacts

## About manylinux and musllinux

**manylinux** and **musllinux** are standardized Docker images maintained by the Python Packaging Authority (PyPA):

- **Purpose**: Build portable binary wheels that work across many Linux distributions
- **Standards**: 
  - `manylinux_2_28`: Compatible with glibc 2.28+ (Ubuntu 20.04+, RHEL 8+, Debian 10+)
  - `musllinux_1_2`: Compatible with musl 1.2+ (Alpine 3.13+)
- **Pre-installed**: Multiple Python versions (3.8-3.14), compilers, build tools
- **Official**: Maintained by PyPA at https://github.com/pypa/manylinux

**Benefits**:
- ✅ Binary compatibility guaranteed
- ✅ No need to install Python versions manually
- ✅ Audited with `auditwheel` to verify portability
- ✅ Used by major Python packages (numpy, pandas, cryptography, etc.)

## Maintenance

### Update Container Images

Re-import images monthly or when new Python versions are released:

```bash
cd containers
./import-python-build-images.sh
```

### Modify Python Versions

Edit `.pipeline/templates/build-python-wheels-template.yml`:

```yaml
parameters:
- name: pythonVersions
  type: object
  default:
  - '3.10'
  - '3.11'
  - '3.12'
  - '3.13'
  - '3.14'  # Add/remove versions here
```

## Troubleshooting

### Container image not found
```bash
# Re-import images
cd containers
./import-python-wheel-build-images.sh
```

### Python version not building
Check the container has that Python version:
```bash
docker run --rm tdslibrs.azurecr.io/python-build/manylinux_2_28_x86_64:latest \
  ls -la /opt/python
```

### ACR authentication fails
```bash
az acr login -n tdslibrs
```

## References

- [PyPA manylinux](https://github.com/pypa/manylinux)
- [PEP 600 - manylinux_2_28](https://peps.python.org/pep-0600/)
- [PEP 656 - musllinux](https://peps.python.org/pep-0656/)
- [Maturin documentation](https://www.maturin.rs/)
