#!/bin/bash
#
# Validate the Kerberos test environment locally
# This script runs a lightweight validation to verify Kerberos ticket acquisition works.
#
# NOTE: This is a FAST validation (~5s per distro) that only tests kinit/klist.
#       It does NOT install Rust or run the full test suite.
#       For full tests, use run-kerberos-tests.sh or the CI pipeline.
#
# Usage:
#   ./validate-local.sh                    # Run with default profile (ubuntu22)
#   ./validate-local.sh alpine318          # Run with specific profile
#   ./validate-local.sh all                # Run all profiles sequentially
#   ./validate-local.sh --setup            # Start DC+SQL only (reusable infrastructure)
#   ./validate-local.sh --stop             # Stop DC+SQL (keeps volumes)
#   ./validate-local.sh --cleanup          # Full cleanup (removes volumes too)
#
# Efficient workflow for testing multiple distros:
#   ./validate-local.sh --setup            # Start infrastructure once (~45s)
#   ./validate-local.sh alpine318          # Test first distro (~5s)
#   ./validate-local.sh alpine319          # Test second distro (~5s)
#   ./validate-local.sh ubuntu22           # Test third distro (~5s)
#   ./validate-local.sh --stop             # Stop when done
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Use lightweight validation compose file (no Rust installation)
COMPOSE_FILE="docker-compose-validation.yml"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# All available profiles
ALL_PROFILES="ubuntu22 ubuntu24 alpine318 alpine319 alpine320 alpine321 debian rhel9 oracle9"

# =============================================================================
# Helper Functions
# =============================================================================

check_infrastructure_running() {
    # Check if DC and SQL are running and healthy
    local dc_running=$(docker ps --filter "name=samba-dc" --filter "status=running" --format '{{.Names}}' 2>/dev/null)
    local sql_running=$(docker ps --filter "name=mssql-kerberos" --filter "status=running" --format '{{.Names}}' 2>/dev/null)
    
    if [ -n "$dc_running" ] && [ -n "$sql_running" ]; then
        return 0  # Running
    fi
    return 1  # Not running
}

check_dc_healthy() {
    local status=$(docker inspect --format='{{.State.Health.Status}}' samba-dc 2>/dev/null || echo "not found")
    [ "$status" = "healthy" ]
}

wait_for_infrastructure() {
    # Wait for Samba DC
    echo "Waiting for Samba AD DC to be healthy..."
    local max_wait=300
    local waited=0
    while [ $waited -lt $max_wait ]; do
        if check_dc_healthy; then
            echo -e "${GREEN}✓ Samba AD DC is healthy${NC}"
            break
        fi
        echo -n "."
        sleep 5
        waited=$((waited + 5))
    done

    if ! check_dc_healthy; then
        echo -e "${RED}ERROR: Samba DC failed to become healthy${NC}"
        docker compose -f "$COMPOSE_FILE" logs dc
        exit 1
    fi

    # Wait for SQL Server
    echo "Waiting for SQL Server..."
    max_wait=120
    waited=0
    while [ $waited -lt $max_wait ]; do
        if docker exec mssql-kerberos /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$KERBEROS_SA_PASSWORD" -C -Q "SELECT 1" &>/dev/null; then
            echo -e "${GREEN}✓ SQL Server is ready${NC}"
            return 0
        fi
        echo -n "."
        sleep 5
        waited=$((waited + 5))
    done

    echo -e "${RED}ERROR: SQL Server not responding${NC}"
    docker compose -f "$COMPOSE_FILE" logs mssql
    exit 1
}

start_infrastructure() {
    echo -e "${YELLOW}Starting Samba DC and SQL Server...${NC}"
    
    # Ensure credentials exist
    if [ ! -f .env ]; then
        echo "Generating credentials..."
        ./generate-env.sh
    fi
    source .env
    
    # Build and start
    docker compose -f "$COMPOSE_FILE" build dc mssql
    docker compose -f "$COMPOSE_FILE" up -d dc mssql
    
    wait_for_infrastructure
    
    # Configure Kerberos (idempotent)
    echo ""
    echo -e "${YELLOW}Configuring Kerberos authentication...${NC}"
    ./configure-kerberos.sh
    
    echo ""
    echo -e "${GREEN}✓ Infrastructure ready${NC}"
}

stop_infrastructure() {
    echo -e "${YELLOW}Stopping infrastructure (keeping volumes)...${NC}"
    docker compose -f "$COMPOSE_FILE" stop dc mssql 2>/dev/null || true
    echo -e "${GREEN}✓ Infrastructure stopped${NC}"
    echo ""
    echo "To restart: ./validate-local.sh --setup"
    echo "To cleanup: ./validate-local.sh --cleanup"
}

cleanup_all() {
    echo -e "${YELLOW}Cleaning up everything...${NC}"
    for p in $ALL_PROFILES; do
        docker compose -f "$COMPOSE_FILE" --profile "$p" down -v 2>/dev/null || true
        docker compose -f docker-compose-matrix.yml --profile "$p" down -v 2>/dev/null || true
    done
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    docker compose -f docker-compose-matrix.yml down -v 2>/dev/null || true
    docker compose -f docker-compose-samba.yml down -v 2>/dev/null || true
    rm -f .env
    echo -e "${GREEN}✓ Cleanup complete${NC}"
}

test_profile() {
    local profile=$1
    local container_name="kerberos-client-$profile"
    
    echo ""
    echo -e "${BLUE}----------------------------------------------"
    echo "  Testing: $profile"
    echo -e "----------------------------------------------${NC}"
    
    # Build client image
    docker compose -f "$COMPOSE_FILE" --profile "$profile" build 2>/dev/null
    
    # Start client container
    docker compose -f "$COMPOSE_FILE" --profile "$profile" up -d --no-recreate
    sleep 3
    
    if ! docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
        echo -e "${RED}ERROR: Client container not running${NC}"
        return 1
    fi
    
    # Configure krb5.conf in client
    docker exec "$container_name" bash -c 'cat > /etc/krb5.conf << EOF
[libdefaults]
    default_realm = EXAMPLE.LOCAL
    dns_lookup_realm = false
    dns_lookup_kdc = false
    rdns = false

[realms]
    EXAMPLE.LOCAL = {
        kdc = dc.example.local
        admin_server = dc.example.local
    }

[domain_realm]
    .example.local = EXAMPLE.LOCAL
    example.local = EXAMPLE.LOCAL
EOF'
    
    # Acquire ticket and verify
    if docker exec "$container_name" bash -c "echo '$KERBEROS_TEST_USER_PASSWORD' | kinit testuser@EXAMPLE.LOCAL" 2>/dev/null; then
        docker exec "$container_name" klist
        echo -e "${GREEN}✓ Kerberos ticket acquired for $profile${NC}"
        
        # Stop and remove ONLY the client container (keep DC + SQL running)
        docker stop "$container_name" 2>/dev/null || true
        docker rm -f "$container_name" 2>/dev/null || true
        return 0
    else
        echo -e "${RED}✗ Failed to acquire ticket for $profile${NC}"
        docker stop "$container_name" 2>/dev/null || true
        docker rm -f "$container_name" 2>/dev/null || true
        return 1
    fi
}

# =============================================================================
# Main Logic
# =============================================================================

# Handle special flags
case "${1:-}" in
    --cleanup)
        cleanup_all
        exit 0
        ;;
    --setup)
        if check_infrastructure_running && check_dc_healthy; then
            echo -e "${GREEN}Infrastructure already running and healthy${NC}"
            exit 0
        fi
        start_infrastructure
        echo ""
        echo "Infrastructure is ready. Run tests with:"
        echo "  ./validate-local.sh <profile>    # e.g., alpine318, ubuntu22"
        echo "  ./validate-local.sh all          # test all distros"
        exit 0
        ;;
    --stop)
        stop_infrastructure
        exit 0
        ;;
    --help|-h)
        echo "Usage: $0 [profile|all|--setup|--stop|--cleanup]"
        echo ""
        echo "Profiles: $ALL_PROFILES"
        echo ""
        echo "Flags:"
        echo "  --setup    Start DC+SQL infrastructure (reusable)"
        echo "  --stop     Stop infrastructure (keeps data)"
        echo "  --cleanup  Full cleanup (removes everything)"
        echo ""
        echo "Efficient workflow:"
        echo "  $0 --setup      # Start once (~45s)"
        echo "  $0 alpine318    # Test distro (~5s each)"
        echo "  $0 alpine319"
        echo "  $0 --stop       # Stop when done"
        exit 0
        ;;
esac

# Determine profiles to test
PROFILE="${1:-ubuntu22}"
if [ "$PROFILE" == "all" ]; then
    PROFILES_TO_TEST="$ALL_PROFILES"
else
    PROFILES_TO_TEST="$PROFILE"
fi

echo -e "${BLUE}=============================================="
echo "  Kerberos Test Environment Validation"
echo "  Profile(s): $PROFILES_TO_TEST"
echo -e "==============================================${NC}"
echo ""

START_TIME=$(date +%s)

# Check if infrastructure is already running
if check_infrastructure_running && check_dc_healthy; then
    echo -e "${GREEN}✓ Using existing infrastructure${NC}"
    source .env
    INFRA_REUSED=true
else
    echo -e "${YELLOW}Starting infrastructure...${NC}"
    start_infrastructure
    INFRA_REUSED=false
fi
echo ""

# Test each profile
PASSED_PROFILES=""
FAILED_PROFILES=""

for profile in $PROFILES_TO_TEST; do
    if test_profile "$profile"; then
        PASSED_PROFILES="$PASSED_PROFILES $profile"
    else
        FAILED_PROFILES="$FAILED_PROFILES $profile"
    fi
done

# Summary
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo -e "${BLUE}=============================================="
echo "  Validation Summary"
echo -e "==============================================${NC}"
echo ""
echo "  Time elapsed: ${ELAPSED}s"
if [ "$INFRA_REUSED" = true ]; then
    echo "  Infrastructure: reused existing"
else
    echo "  Infrastructure: started fresh"
fi
echo ""

if [ -n "$PASSED_PROFILES" ]; then
    echo -e "${GREEN}  ✅ Passed:$PASSED_PROFILES${NC}"
fi

if [ -n "$FAILED_PROFILES" ]; then
    echo -e "${RED}  ❌ Failed:$FAILED_PROFILES${NC}"
    echo ""
    exit 1
fi

echo ""
echo -e "${GREEN}All validations passed!${NC}"
echo ""
echo "Infrastructure is still running. Next steps:"
echo "  - Test another distro: ./validate-local.sh <profile>"
echo "  - Stop infrastructure: ./validate-local.sh --stop"
echo "  - Full cleanup:        ./validate-local.sh --cleanup"
