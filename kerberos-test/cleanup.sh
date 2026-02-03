#!/bin/bash
#
# Cleanup script - removes all containers and volumes
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Stopping and removing Kerberos test environment...${NC}"

docker compose -f docker-compose-samba.yml down -v 2>/dev/null || true

echo -e "${GREEN}✓ Environment cleaned up${NC}"
echo ""
echo "To restart, run: ./setup.sh"
