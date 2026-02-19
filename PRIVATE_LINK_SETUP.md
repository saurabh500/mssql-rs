# Azure SQL Private Link Setup for ACI Testing

This document describes the Private Link configuration created for the `mssqlrustlibtest` Azure SQL Server, enabling Azure Container Instances (ACI) to connect to the database over a private network path — no public internet traversal.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  VNet: vnet-sqldrivers-trusted-westus3  (100.0.0.0/16)            │
│  Resource Group: rg-sqldrivers-shared                              │
│                                                                     │
│  ┌──────────────────────────────────┐                               │
│  │ Subnet: default                  │                               │
│  │ (No delegation — private         │                               │
│  │  endpoints live here)            │                               │
│  │                                  │                               │
│  │  ┌────────────────────────────┐  │                               │
│  │  │ sql-private-endpoint       │  │                               │
│  │  │ IP: 100.0.0.100            │──┼───► Azure SQL: mssqlrustlibtest
│  │  │ Group: sqlServer           │  │     (rust-lib-rg)             │
│  │  └────────────────────────────┘  │                               │
│  └──────────────────────────────────┘                               │
│                                                                     │
│  ┌──────────────────────────────────┐                               │
│  │ Subnet: default2                 │                               │
│  │ Delegation: ContainerInstance    │                               │
│  │                                  │                               │
│  │  ┌────────────────────────────┐  │                               │
│  │  │ aci-rust-sql-test          │  │   DNS query:                  │
│  │  │ IP: 100.0.6.6              │──┼───► mssqlrustlibtest          │
│  │  │                            │  │     .database.windows.net     │
│  │  └────────────────────────────┘  │     resolves to 100.0.0.100   │
│  └──────────────────────────────────┘                               │
│                                                                     │
│  Private DNS Zone: privatelink.database.windows.net                 │
│  A Record: mssqlrustlibtest → 100.0.0.100                          │
└─────────────────────────────────────────────────────────────────────┘
```

## Pre-Existing Infrastructure

Before this setup, the following resources already existed:

| Resource | Resource Group | Details |
|---|---|---|
| **Azure SQL Server** `mssqlrustlibtest` | `rust-lib-rg` | FQDN: `mssqlrustlibtest.database.windows.net`, DB: `librarytest` (Standard SKU) |
| **VNet** `vnet-sqldrivers-trusted-westus3` | `rg-sqldrivers-shared` | Address space: `100.0.0.0/16` |
| **Subnet** `default` | (in above VNet) | No delegation, private endpoint network policies disabled — used by existing blob/AKV private endpoints |
| **Subnet** `default2` | (in above VNet) | Delegated to `Microsoft.ContainerInstance/containerGroups` — used by existing ACI containers |

No new VNets or subnets were created. All resources were placed into the existing shared networking infrastructure.

## Step-by-Step Setup

### Step 1: Create the Private Endpoint

```bash
az network private-endpoint create \
  --resource-group rust-lib-rg \
  --name sql-private-endpoint \
  --subnet /subscriptions/654fffd0-d02d-4894-b1b7-e2dfbc44a665/resourceGroups/rg-sqldrivers-shared/providers/Microsoft.Network/virtualNetworks/vnet-sqldrivers-trusted-westus3/subnets/default \
  --private-connection-resource-id /subscriptions/654fffd0-d02d-4894-b1b7-e2dfbc44a665/resourceGroups/rust-lib-rg/providers/Microsoft.Sql/servers/mssqlrustlibtest \
  --group-id sqlServer \
  --connection-name sql-pe-connection \
  --location westus3
```

**What this does:**

- Creates a network interface (NIC) in the `default` subnet with a private IP (`100.0.0.100`) from the VNet's address space.
- Establishes a private connection from that NIC to the Azure SQL Server's `sqlServer` sub-resource.
- After this, any traffic sent to `100.0.0.100:1433` reaches the SQL Server over the Azure backbone — never leaving the VNet.
- The `--group-id sqlServer` specifies which sub-resource of the PaaS service to expose. For Azure SQL, `sqlServer` exposes the TDS endpoint (port 1433).
- The private endpoint is in `rust-lib-rg` but references a subnet in `rg-sqldrivers-shared` — this cross-resource-group reference is supported.

### Step 2: Create the Private DNS Zone

```bash
az network private-dns zone create \
  --resource-group rust-lib-rg \
  --name privatelink.database.windows.net
```

**What this does:**

- Creates an Azure Private DNS zone named `privatelink.database.windows.net`.
- This is the well-known zone name that Azure SQL uses for private link DNS resolution. When Azure DNS receives a query for `mssqlrustlibtest.database.windows.net`, it returns a CNAME to `mssqlrustlibtest.privatelink.database.windows.net`. If a private DNS zone for that name is linked to the querying VNet, the private IP is returned instead of the public IP.
- The zone is empty at creation — it gets an A record in Step 4 via the DNS zone group.

### Step 3: Link the DNS Zone to the VNet

```bash
az network private-dns link vnet create \
  --resource-group rust-lib-rg \
  --zone-name privatelink.database.windows.net \
  --name sql-dns-link \
  --virtual-network /subscriptions/654fffd0-d02d-4894-b1b7-e2dfbc44a665/resourceGroups/rg-sqldrivers-shared/providers/Microsoft.Network/virtualNetworks/vnet-sqldrivers-trusted-westus3 \
  --registration-enabled false
```

**What this does:**

- Links the private DNS zone to the shared VNet. Without this link, VMs and containers in the VNet would not query this zone — DNS queries for `privatelink.database.windows.net` would fall through to public DNS and return the public IP.
- `--registration-enabled false` means this zone is for resolution only. Auto-registration (where VMs automatically get DNS records) is not needed here — the A record is managed by the DNS zone group in Step 4.
- After this, any DNS query from within `vnet-sqldrivers-trusted-westus3` for `*.privatelink.database.windows.net` will check this zone first.

### Step 4: Create the DNS Zone Group

```bash
az network private-endpoint dns-zone-group create \
  --resource-group rust-lib-rg \
  --endpoint-name sql-private-endpoint \
  --name sql-dns-zone-group \
  --private-dns-zone /subscriptions/654fffd0-d02d-4894-b1b7-e2dfbc44a665/resourceGroups/rust-lib-rg/providers/Microsoft.Network/privateDnsZones/privatelink.database.windows.net \
  --zone-name database
```

**What this does:**

- Associates the private endpoint with the private DNS zone. Azure automatically creates (and manages the lifecycle of) an A record in the zone:
  - `mssqlrustlibtest` → `100.0.0.100`
- If the private endpoint's IP changes or the endpoint is deleted, the A record is automatically updated/removed.
- `--zone-name database` is a logical label for this zone within the group — it's not the DNS zone name itself.
- This is the glue that makes the whole DNS chain work: `mssqlrustlibtest.database.windows.net` → CNAME `mssqlrustlibtest.privatelink.database.windows.net` → A record `100.0.0.100`.

### Step 5: Deploy the ACI Container

```bash
az container create \
  --resource-group rust-lib-rg \
  --name aci-rust-sql-test \
  --image mcr.microsoft.com/azure-cli:latest \
  --os-type Linux \
  --vnet /subscriptions/654fffd0-d02d-4894-b1b7-e2dfbc44a665/resourceGroups/rg-sqldrivers-shared/providers/Microsoft.Network/virtualNetworks/vnet-sqldrivers-trusted-westus3 \
  --subnet default2 \
  --location westus3 \
  --cpu 1 \
  --memory 1.5 \
  --command-line "tail -f /dev/null" \
  --restart-policy Never
```

**What this does:**

- Deploys a Linux container into the `default2` subnet, which is already delegated to `Microsoft.ContainerInstance/containerGroups`.
- The container gets a private IP (`100.0.6.6`) from the VNet's address space — it's VNet-injected with no public IP.
- `--command-line "tail -f /dev/null"` keeps the container alive indefinitely without doing work (useful for exec-ing into it for testing).
- `--restart-policy Never` means the container won't restart if it exits.
- Because the container is in the same VNet as the private endpoint and DNS zone link, it can resolve `mssqlrustlibtest.database.windows.net` to the private IP.

## How DNS Resolution Works (End-to-End)

When the ACI container queries `mssqlrustlibtest.database.windows.net`:

1. **Azure DNS** returns a CNAME: `mssqlrustlibtest.privatelink.database.windows.net`
2. Since `privatelink.database.windows.net` is a **private DNS zone linked to this VNet**, Azure checks the zone
3. The zone has an A record: `mssqlrustlibtest` → `100.0.0.100`
4. The container connects to `100.0.0.100:1433` — routed internally within the VNet to the private endpoint NIC
5. The private endpoint forwards the traffic to Azure SQL over the Azure backbone

## Verification

From inside the ACI container:

```
$ getent hosts mssqlrustlibtest.database.windows.net
100.0.0.100     mssqlrustlibtest.privatelink.database.windows.net mssqlrustlibtest.database.windows.net
```

This confirms the FQDN resolves to the private endpoint IP (`100.0.0.100`), not a public IP.

## Resources Created

| Resource | Type | Resource Group | Notes |
|---|---|---|---|
| `sql-private-endpoint` | Private Endpoint | `rust-lib-rg` | NIC in `default` subnet, IP `100.0.0.100` |
| `privatelink.database.windows.net` | Private DNS Zone | `rust-lib-rg` | A record: `mssqlrustlibtest` → `100.0.0.100` |
| `sql-dns-link` | DNS VNet Link | `rust-lib-rg` | Links zone to `vnet-sqldrivers-trusted-westus3` |
| `sql-dns-zone-group` | DNS Zone Group | `rust-lib-rg` | Auto-manages A record lifecycle |
| `aci-rust-sql-test` | Container Instance | `rust-lib-rg` | Linux, `default2` subnet, IP `100.0.6.6` |

## Cleanup

To remove all resources created by this setup:

```bash
# Delete ACI container
az container delete --resource-group rust-lib-rg --name aci-rust-sql-test --yes

# Delete DNS zone group (removes auto-managed A record)
az network private-endpoint dns-zone-group delete \
  --resource-group rust-lib-rg \
  --endpoint-name sql-private-endpoint \
  --name sql-dns-zone-group

# Delete DNS VNet link
az network private-dns link vnet delete \
  --resource-group rust-lib-rg \
  --zone-name privatelink.database.windows.net \
  --name sql-dns-link --yes

# Delete private DNS zone
az network private-dns zone delete \
  --resource-group rust-lib-rg \
  --name privatelink.database.windows.net --yes

# Delete private endpoint
az network private-endpoint delete \
  --resource-group rust-lib-rg \
  --name sql-private-endpoint
```

## Impact on Existing Infrastructure

- **No new VNets or subnets** were created — all resources use the existing shared VNet.
- **No routing changes** — the private endpoint is a NIC in an existing subnet.
- **No NSG modifications** — the `default` subnet already had private endpoint network policies disabled.
- **Public access unchanged** — the SQL Server still accepts public connections. To restrict to private-only, you would run `az sql server update --resource-group rust-lib-rg --name mssqlrustlibtest --set publicNetworkAccess="Disabled"`, but this would break any existing clients connecting over the public endpoint.
