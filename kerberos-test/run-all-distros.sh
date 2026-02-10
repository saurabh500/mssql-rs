#!/bin/bash
# Run Kerberos tests on all Linux distributions in the matrix
# Usage: ./run-all-distros.sh [profiles...]
#
# Examples:
#   ./run-all-distros.sh              # Run all profiles
#   ./run-all-distros.sh ubuntu22     # Run only ubuntu22
#   ./run-all-distros.sh alpine318 debian ubuntu22  # Run specific profiles

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# All available profiles (matching CI matrix from validation-pipeline.yml)
ALL_PROFILES="alpine318 alpine319 alpine320 alpine321 debian ubuntu22 ubuntu24 rhel9 oracle9"

# Use provided profiles or default to all
if [ $# -gt 0 ]; then
    PROFILES="$*"
else
    PROFILES="$ALL_PROFILES"
fi

RESULTS=()

echo "=============================================="
echo "Kerberos Test Matrix Runner"
echo "Testing: $PROFILES"
echo "=============================================="

echo ""
echo "Step 1: Stop any existing containers..."
echo "----------------------------------------------"
docker compose -f docker-compose-matrix.yml --profile all down -v 2>/dev/null || true
docker compose -f docker-compose-samba.yml down -v 2>/dev/null || true

echo ""
echo "Step 2: Build client images..."
echo "----------------------------------------------"
for profile in $PROFILES; do
    echo "Building client-$profile..."
    docker compose -f docker-compose-matrix.yml --profile $profile build client-$profile 2>/dev/null || \
        docker compose -f docker-compose-matrix.yml build client-$profile
done

echo ""
echo "Step 3: Start infrastructure (Samba DC + SQL Server)..."
echo "----------------------------------------------"
docker compose -f docker-compose-matrix.yml up -d dc mssql

echo ""
echo "Waiting for Samba DC to be healthy..."
timeout 300 bash -c 'until docker compose -f docker-compose-matrix.yml ps dc | grep -q "healthy"; do echo -n "."; sleep 5; done'
echo " ✓ Samba DC healthy"

echo ""
echo "Waiting for SQL Server to be ready..."
sleep 30  # Give SQL Server time to start

echo ""
echo "Step 4: Configure Kerberos on SQL Server..."
echo "----------------------------------------------"
./configure-kerberos.sh

echo ""
echo "Step 5: Run tests on each distro..."
echo "----------------------------------------------"

for profile in $PROFILES; do
    echo ""
    echo "====== Testing on $profile ======"
    
    # Start this profile's client
    docker compose -f docker-compose-matrix.yml --profile $profile up -d
    
    # Wait for container to be ready
    sleep 5
    
    # Run tests
    if ./run-kerberos-tests.sh $profile; then
        RESULTS+=("$profile: ✅ PASSED")
    else
        RESULTS+=("$profile: ❌ FAILED")
    fi
    
    # Stop this client (keep infrastructure running)
    docker compose -f docker-compose-matrix.yml --profile $profile stop
done

echo ""
echo "=============================================="
echo "Test Results Summary"
echo "=============================================="
for result in "${RESULTS[@]}"; do
    echo "  $result"
done
echo "=============================================="

# Check if any tests failed
failed=0
for result in "${RESULTS[@]}"; do
    if echo "$result" | grep -q "FAILED"; then
        failed=1
    fi
done

if [ $failed -eq 1 ]; then
    echo "❌ Some tests failed!"
    exit 1
else
    echo "✅ All tests passed!"
    exit 0
fi
