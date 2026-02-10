#!/bin/bash
# docker-cargo-run.sh - Wrapper for docker run that injects CARGO registry tokens
#
# Usage: docker-cargo-run.sh [docker-run-args...] <image> <command> [command-args...]
#
# This script prepends the cargo registry environment variables to any docker run command.
# It also prints debug info about the token status before running.
#
# Example:
#   .pipeline/scripts/docker-cargo-run.sh --rm -v $PWD:/workspace -w /workspace \
#     my-image:latest /workspace/scripts/build.sh

set -euo pipefail

# Debug: Echo cargo registry token status (masked for security)
echo "=== Cargo Registry Token Status ==="
echo "CARGO_REGISTRIES_MSSQL_RS_PUBLIC_TOKEN is set: $([ -n "${CARGO_REGISTRIES_MSSQL_RS_PUBLIC_TOKEN:-}" ] && echo 'YES' || echo 'NO')"
echo "CARGO_REGISTRIES_MSSQL_RS_PUBLIC_TOKEN length: ${#CARGO_REGISTRIES_MSSQL_RS_PUBLIC_TOKEN}"
echo "CARGO_REGISTRIES_MSSQL_RS_TOKEN is set: $([ -n "${CARGO_REGISTRIES_MSSQL_RS_TOKEN:-}" ] && echo 'YES' || echo 'NO')"
echo "CARGO_REGISTRIES_MSSQL_RS_TOKEN length: ${#CARGO_REGISTRIES_MSSQL_RS_TOKEN}"
echo "===================================="

exec docker run \
  -e CARGO_REGISTRIES_MSSQL_RS_PUBLIC_TOKEN="$CARGO_REGISTRIES_MSSQL_RS_PUBLIC_TOKEN" \
  -e CARGO_REGISTRIES_MSSQL_RS_PUBLIC_CREDENTIAL_PROVIDER="cargo:token" \
  -e CARGO_REGISTRIES_MSSQL_RS_TOKEN="$CARGO_REGISTRIES_MSSQL_RS_TOKEN" \
  -e CARGO_REGISTRIES_MSSQL_RS_CREDENTIAL_PROVIDER="cargo:token" \
  "$@"
