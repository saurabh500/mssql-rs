#!/bin/bash
set -e

REALM="EXAMPLE.LOCAL"
DOMAIN="EXAMPLE"
# Password must be set via environment variable from docker-compose
if [ -z "$KERBEROS_ADMIN_PASSWORD" ]; then
    echo "ERROR: KERBEROS_ADMIN_PASSWORD environment variable not set"
    echo "Run ./generate-env.sh and ensure docker-compose sources .env"
    exit 1
fi
ADMIN_PASSWORD="$KERBEROS_ADMIN_PASSWORD"
DNS_FORWARDER="8.8.8.8"

echo "=== Initializing Samba AD Domain Controller ==="

# Check if already provisioned
if [ ! -f /var/lib/samba/private/sam.ldb ]; then
    echo "Provisioning Samba AD DC..."
    
    # Remove existing configuration
    rm -f /etc/samba/smb.conf
    rm -rf /var/lib/samba/*
    rm -rf /var/cache/samba/*
    rm -f /etc/krb5.conf
    
    # Provision the domain
    samba-tool domain provision \
        --use-rfc2307 \
        --realm=$REALM \
        --domain=$DOMAIN \
        --server-role=dc \
        --dns-backend=SAMBA_INTERNAL \
        --adminpass="$ADMIN_PASSWORD"
    
    # Copy Kerberos config
    cp /var/lib/samba/private/krb5.conf /etc/krb5.conf
    
    echo "Samba AD DC provisioned successfully!"
else
    echo "Samba AD DC already provisioned."
fi

# Start Samba
echo "Starting Samba AD DC..."
exec /usr/sbin/samba -i --debug-stdout
