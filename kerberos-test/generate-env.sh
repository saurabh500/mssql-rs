#!/bin/bash
# Generate random credentials for Kerberos test environment
# This script creates a .env file with generated passwords
# The .env file is gitignored and should never be committed

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

# Generate a random password meeting SQL Server complexity requirements
# At least 8 chars, uppercase, lowercase, digit, special char
# Note: Avoid shell metacharacters in passwords as they cause issues when sourcing .env files
generate_password() {
    local length=${1:-16}
    # Use /dev/urandom for randomness, ensure complexity
    local upper=$(tr -dc 'A-Z' < /dev/urandom | head -c 2)
    local lower=$(tr -dc 'a-z' < /dev/urandom | head -c 6)
    local digit=$(tr -dc '0-9' < /dev/urandom | head -c 4)
    # Use special characters that are safe in shell, docker-compose, and SQL Server
    # Avoid: $ ` \ ' " | ; & ( ) { } < > : ? * [ ] ! # ^ space tab newline
    # Safe: @ % - _ = + . ,
    local special='@%-_=+.,'
    local special_char=${special:$((RANDOM % ${#special})):1}
    
    # Combine and shuffle
    echo "${upper}${lower}${digit}${special_char}" | fold -w1 | shuf | tr -d '\n'
}

# Check if .env already exists
if [ -f "$ENV_FILE" ]; then
    echo "Environment file already exists: $ENV_FILE"
    echo "To regenerate, delete it first: rm $ENV_FILE"
    exit 0
fi

echo "Generating credentials for Kerberos test environment..."

# Generate passwords
ADMIN_PASSWORD=$(generate_password 16)
SA_PASSWORD=$(generate_password 16)
TEST_USER_PASSWORD=$(generate_password 16)

# Create .env file
cat > "$ENV_FILE" << EOF
# Auto-generated credentials for Kerberos test environment
# Generated on: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
# DO NOT COMMIT THIS FILE

# Domain Administrator password (used by Samba DC)
KERBEROS_ADMIN_PASSWORD=${ADMIN_PASSWORD}

# SQL Server SA password
KERBEROS_SA_PASSWORD=${SA_PASSWORD}

# Test user password (testuser@EXAMPLE.LOCAL)
KERBEROS_TEST_USER_PASSWORD=${TEST_USER_PASSWORD}

# Domain configuration (these can be hardcoded as they're not secrets)
KERBEROS_REALM=EXAMPLE.LOCAL
KERBEROS_DOMAIN=example.local
KERBEROS_NETBIOS=EXAMPLE
EOF

chmod 600 "$ENV_FILE"

echo "✓ Generated credentials in: $ENV_FILE"
echo ""
echo "Credentials summary:"
echo "  Domain Admin:    Administrator@EXAMPLE.LOCAL"
echo "  SQL SA:          sa"
echo "  Test User:       testuser@EXAMPLE.LOCAL"
echo ""
echo "Passwords are stored in $ENV_FILE (gitignored)"
