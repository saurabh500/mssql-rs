#!/bin/bash
set -e

REALM="EXAMPLE.LOCAL"
DOMAIN="EXAMPLE"
DC_HOST="dc.example.local"
# Password must be set via environment variable from docker-compose
if [ -z "$KERBEROS_ADMIN_PASSWORD" ]; then
    echo "ERROR: KERBEROS_ADMIN_PASSWORD environment variable not set"
    echo "Run ./generate-env.sh and ensure docker-compose sources .env"
    exit 1
fi
ADMIN_PASSWORD="$KERBEROS_ADMIN_PASSWORD"
SQL_HOSTNAME="sql"

echo "=== SQL Server with AD Integration Startup ==="

# Wait for network to be ready
sleep 5

# Configure Kerberos
echo "Configuring Kerberos..."
cat > /etc/krb5.conf << EOF
[libdefaults]
    default_realm = $REALM
    dns_lookup_realm = false
    dns_lookup_kdc = true
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    rdns = false
    default_ccache_name = FILE:/tmp/krb5cc_%{uid}

[realms]
    $REALM = {
        kdc = $DC_HOST
        admin_server = $DC_HOST
        default_domain = example.local
    }

[domain_realm]
    .example.local = $REALM
    example.local = $REALM
EOF

# Test DNS resolution
echo "Testing DNS resolution..."
until host $DC_HOST; do
    echo "Waiting for DNS to resolve $DC_HOST..."
    sleep 2
done
echo "DNS resolution successful!"

# Test Kerberos connectivity
echo "Testing Kerberos connectivity..."
until echo "$ADMIN_PASSWORD" | kinit Administrator@$REALM 2>/dev/null; do
    echo "Waiting for Kerberos to be available..."
    sleep 5
done
echo "Kerberos authentication successful!"

# Configure SSSD
echo "Configuring SSSD..."
mkdir -p /etc/sssd
cat > /etc/sssd/sssd.conf << EOF
[sssd]
services = nss, pam
config_file_version = 2
domains = $REALM

[domain/$REALM]
id_provider = ad
access_provider = ad
auth_provider = ad
chpass_provider = ad
ad_domain = example.local
krb5_realm = $REALM
realmd_tags = manages-system joined-with-adcli
cache_credentials = True
krb5_store_password_if_offline = True
default_shell = /bin/bash
ldap_id_mapping = True
use_fully_qualified_names = False
fallback_homedir = /home/%u
ad_server = $DC_HOST
ad_hostname = $SQL_HOSTNAME.example.local
EOF

chmod 600 /etc/sssd/sssd.conf

# Configure NSS
echo "Configuring NSS..."
sed -i 's/^passwd:.*/passwd:         files sss/' /etc/nsswitch.conf
sed -i 's/^group:.*/group:          files sss/' /etc/nsswitch.conf
sed -i 's/^shadow:.*/shadow:         files sss/' /etc/nsswitch.conf

# Create keytab directory
mkdir -p /var/opt/mssql/secrets

# Join the domain using adcli
echo "Joining domain $REALM..."
echo "$ADMIN_PASSWORD" | adcli join --domain=example.local \
    --domain-controller=$DC_HOST \
    --host-fqdn=$SQL_HOSTNAME.example.local \
    --host-keytab=/etc/krb5.keytab \
    --login-user=Administrator \
    --stdin-password \
    --verbose 2>&1 || echo "Domain join may have already been done"

# Add MSSQLSvc SPNs to the computer account
echo "Registering SQL Server SPNs..."
echo "$ADMIN_PASSWORD" | adcli add-service \
    --domain=example.local \
    --domain-controller=$DC_HOST \
    --login-user=Administrator \
    --stdin-password \
    --service-name=MSSQLSvc/$SQL_HOSTNAME.example.local \
    --service-name=MSSQLSvc/$SQL_HOSTNAME.example.local:1433 \
    --computer-name=SQL 2>&1 || echo "SPNs may already exist"

# Start SSSD
echo "Starting SSSD..."
mkdir -p /var/run/sssd
rm -f /var/run/sssd/*.pid
sssd -D 2>/dev/null || sssd &
sleep 3

# Verify domain join
echo "Verifying domain membership..."
id testuser@example.local && echo "Domain join verified!" || echo "User lookup not working yet (OK if user not created)"

# Copy the keytab for SQL Server
echo "Configuring SQL Server keytab..."
cp /etc/krb5.keytab /var/opt/mssql/secrets/mssql.keytab
chown mssql:mssql /var/opt/mssql/secrets/mssql.keytab
chmod 400 /var/opt/mssql/secrets/mssql.keytab

echo "=== Keytab contents ==="
klist -ke /var/opt/mssql/secrets/mssql.keytab

# Configure SQL Server keytab
/opt/mssql/bin/mssql-conf set network.kerberoskeytabfile /var/opt/mssql/secrets/mssql.keytab
/opt/mssql/bin/mssql-conf set network.privilegedadaccount SQL

kdestroy

echo ""
echo "=== Starting SQL Server ==="

# Start SQL Server as mssql user
exec /opt/mssql/bin/sqlservr
