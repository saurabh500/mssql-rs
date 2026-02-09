# Cargo Configuration Strategy

This repository uses a dual-configuration approach to support both CI builds and OSS developers.

## For Developers (Default)

The default `.cargo/config.toml` uses **crates.io** directly:
- No authentication required
- Works seamlessly for OSS developers
- No ADO access needed

```bash
# Just clone and build - works out of the box
git clone <repo>
cd mssql-tds
cargo build
```

## For CI/Pipeline

CI builds use `.cargo/config.ci.toml` which configures:
- **Authenticated Azure Artifacts feeds** (`mssql-rs_Public` and `mssql-rs`)
- Source replacement: `crates.io` → `mssql-rs_Public` (ADO feed with crates.io as upstream)
- Authentication via `CargoAuthenticate@0` task (sets `CARGO_REGISTRIES_*_TOKEN` env vars)

### How It Works

1. **Apply CI config**: `.pipeline/scripts/apply-ci-cargo-config.{sh,ps1}` copies `config.ci.toml` → `config.toml`
2. **Authenticate**: `CargoAuthenticate@0` task sets token environment variables
3. **Build**: Cargo uses authenticated ADO feeds

### Pipeline Flow

```yaml
# All build templates include these steps:
- script: bash .pipeline/scripts/apply-ci-cargo-config.sh
  displayName: Apply CI cargo configuration

- task: CargoAuthenticate@0
  inputs:
    configFile: '.cargo/config.toml'
  displayName: Authenticate cargo registries

# Now cargo commands use authenticated feeds
- script: cargo build
```

## Files

| File | Purpose |
|------|---------|
| `.cargo/config.toml` | **Default** - Uses crates.io (for developers) |
| `.cargo/config.ci.toml` | **CI override** - Uses authenticated ADO feeds |
| `.pipeline/scripts/apply-ci-cargo-config.sh` | Applies CI config (Linux/Mac) |
| `.pipeline/scripts/apply-ci-cargo-config.ps1` | Applies CI config (Windows) |

## Benefits

✅ **Zero friction for OSS developers** - Just clone and build  
✅ **Fast CI builds** - Uses ADO artifact cache  
✅ **Secure** - Tokens only exist in CI, not committed to repo  
✅ **Flexible** - Easy to switch between modes

## Troubleshooting

### For Developers
If you see authentication errors, ensure you're using the default config:
```bash
git checkout .cargo/config.toml
```

### For CI
Ensure the apply-ci-cargo-config step runs **before** `CargoAuthenticate@0`:
```yaml
- script: bash .pipeline/scripts/apply-ci-cargo-config.sh
- task: CargoAuthenticate@0
  inputs:
    configFile: '.cargo/config.toml'
```
