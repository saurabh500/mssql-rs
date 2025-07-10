#/bin/bash

echo "Make sure to set the SQL_PASSWORD environment variable before running this script. It will be used to set the SA password for the SQL Server instance."


if [ -z "$SQL_PASSWORD" ]; then
  echo "Error: SQL_PASSWORD environment variable is not set."
  exit 1
fi

if ! docker network ls --format '{{.Name}}' | grep -qw testnet; then
  docker network create testnet
fi

docker run -e "ACCEPT_EULA=Y" -e MSSQL_SA_PASSWORD=$SQL_PASSWORD -p 1433:1433 --network testnet --hostname sql1 -u 0:0 -d mcr.microsoft.com/mssql/server:2025-latest