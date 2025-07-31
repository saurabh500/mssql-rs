#/bin/bash

echo "Make sure to set the SQL_PASSWORD environment variable before running this script. It will be used to set the SA password for the SQL Server instance."

if ! ./scripts/generate_cert.sh; then
  echo "Error: Certificate generation failed."
  exit 1
fi

if [ -z "$SQL_PASSWORD" ]; then
  echo "⚠️  WARN: SQL_PASSWORD environment variable is not set. Generating a random password."
  SQL_PASSWORD=$(openssl rand -base64 12)
  echo "Generated SQL_PASSWORD: $SQL_PASSWORD"
  echo "ℹ️  INFO: Adding password to env file"
  if grep -q "SQL_PASSWORD=" mssql-tds/.env; then
    sed -i "s/^SQL_PASSWORD=.*/SQL_PASSWORD=$SQL_PASSWORD/" mssql-tds/.env
  else
    echo "$SQL_PASSWORD" > /tmp/password
    chmod 600 /tmp/password
  fi
fi

if ! docker network ls --format '{{.Name}}' | grep -qw testnet; then
  docker network create testnet
fi

docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=$SQL_PASSWORD" -p 1433:1433 --hostname sql1 --network testnet -v $PWD/conf/mssql.conf:/var/opt/mssql/mssql.conf -v $PWD/mssql.pem:/etc/ssl/certs/mssql.pem -v $PWD/mssql.key:/etc/ssl/private/mssql.key -u 0:0 -d mcr.microsoft.com/mssql/server:2025-latest

# docker run -e "ACCEPT_EULA=Y" -e MSSQL_SA_PASSWORD=$SQL_PASSWORD -p 1433:1433 --network testnet --hostname sql1 -u 0:0 -d mcr.microsoft.com/mssql/server:2025-latest

echo "Install the CA certificate to the system trust store"
sudo cp ca.crt /usr/local/share/ca-certificates

sudo update-ca-certificates

echo "Verify the server certificate"
sudo openssl verify -CAfile /etc/ssl/certs/ca-certificates.crt mssql.crt