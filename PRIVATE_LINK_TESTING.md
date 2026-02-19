# Azure SQL Private Link + Redirect Testing

## Objective

Validate that the Rust TDS library (`mssql-tds`) correctly handles Azure SQL connection routing (redirect vs proxy) over Private Link, with FedAuth token-based authentication — matching the behavior of Microsoft.Data.SqlClient.

---

## Infrastructure Setup

### Azure Resources

| Resource | Details |
|---|---|
| **SQL Server** | `mssqlrustlibtest.database.windows.net` (westus3) |
| **Database** | `librarytest` |
| **Auth** | AAD-only (no SQL auth) |
| **AAD Admin** | `singhsaura@microsoft.com` (oid: `d0717ec2-f84c-4489-8f32-c087c6fce7f0`) |
| **Resource Group** | `rust-lib-rg` |
| **Subscription** | `654fffd0-d02d-4894-b1b7-e2dfbc44a665` |

### Networking

| Resource | Details |
|---|---|
| **VNet** | `vnet-sqldrivers-trusted-westus3` (`100.0.0.0/16`) in `rg-sqldrivers-shared` |
| **Private Endpoint** | `sql-private-endpoint` → IP `100.0.0.100` in `default` subnet |
| **Private DNS Zone** | `privatelink.database.windows.net` linked to the VNet |
| **ACI Subnet** | `default2` (delegated to `Microsoft.ContainerInstance/containerGroups`) |

### Container (ACI)

| Property | Value |
|---|---|
| **Name** | `aci-rust-sql-test` |
| **Image** | `tdslibrs.azurecr.io/aci-rust-sql-test:cc17` |
| **Base** | `python:3.14-slim` + `mssql_py_core` wheel (PyO3 bindings to Rust TDS) |
| **Identity** | User-assigned MI `PipelinesUsageIndentity` |
| **MI Client ID** | `d5c2ef7c-cb1b-4be6-b2bd-19ca351aed80` |
| **MI Principal ID** | `3491bd0f-0e78-44df-b761-17e1ed6253a6` |

---

## Connection Policies Tested

Azure SQL supports three connection policies:

| Policy | Behavior |
|---|---|
| **Default** | Redirect inside Azure, Proxy from outside |
| **Redirect** | Always redirect (client connects to gateway on 1433, gets redirected to backend node on port 11000–11999 or similar) |
| **Proxy** | Always proxy through gateway on port 1433 (no redirect) |

Over Private Link, the private endpoint IP (`100.0.0.100`) resolves the gateway. On redirect, the backend node is also reachable via the same private endpoint.

---

## Experiment 1: Redirect Policy + Externally Injected User Token

### Setup
- Connection policy: **Default** (redirect for intra-Azure traffic)
- Token: `az account get-access-token --resource "https://database.windows.net/"` for `singhsaura@microsoft.com`
- Token injected as `access_token` in connection config

### Result: FAILED — both Rust and .NET SqlClient

**Rust TDS from ACI:**
```
Connected to 100.0.0.100:1433
Login → Received Redirection to port 3238
Reconnecting to redirected host on port 3238...
Connected to 100.0.0.100:3238
ERROR: Login failed for user '<token-identified principal>'
```

**Microsoft.Data.SqlClient 6.0.2 from ACI (same token):**
```
Error: Login failed for user '<token-identified principal>'
```

Both drivers fail identically on the redirected backend connection with error 18456. This is **not** a Rust driver bug — SqlClient exhibits the same behavior.

### Root Cause (suspected)
Externally acquired tokens (via `az account get-access-token`) may not work correctly when re-presented to a backend node after redirect. The gateway may need to re-acquire or transform the token for the backend, which doesn't happen when the client injects a pre-acquired token directly.

---

## Experiment 2: Redirect Policy + MI Token from IMDS

### Setup
- Connection policy: **Default** (redirect)
- Token: Acquired inside ACI via IMDS at `http://169.254.169.254/metadata/identity/oauth2/token`
- MI: `PipelinesUsageIndentity` (user-assigned)

### Result: PARTIAL — TDS auth succeeded on redirect, but DB access denied

```
Connected to 100.0.0.100:1433
Login → Received Redirection to port 3238
Reconnecting to redirected host on port 3238...
Connected to 100.0.0.100:3238
ERROR: The server principal "d5c2ef7c-...@72f988bf-..." is not able to access
the database "librarytest" under the current security context.
```

The MI token **authenticated successfully on the redirected backend node** (unlike the user token which got 18456). The failure was purely an authorization issue — the MI didn't have database permissions.

---

## Experiment 3: Proxy Policy + MI Token from IMDS ✅

### Setup
- Connection policy: **Proxy**
- Token: Acquired inside ACI via IMDS
- MI user created in DB with explicit SID matching the client_id

### Result: SUCCESS

```
DNS: mssqlrustlibtest.privatelink.database.windows.net -> 100.0.0.100
Token acquired! Length: 2047 chars
Connected to 100.0.0.100:1433 (no redirect)
CONNECTED!
SELECT 1 = (1,)
Server: mssqlrustlibtest, DB: librarytest
User: d5c2ef7c-cb1b-4be6-b2bd-19ca351aed80@72f988bf-86f1-41af-91ab-2d7cd011db47
Transport: TCP, Client IP: 100.0.6.6
Driver: Core Microsoft SqlClient Data Pr
DONE - SUCCESS
```

---

## Experiment 4: Proxy Policy + SqlClient ActiveDirectoryDefault ✅

### Setup
- Connection policy: **Proxy**
- Auth: `Authentication=ActiveDirectoryDefault` (SqlClient acquires token internally via MSAL)
- Running locally from dev machine

### Result: SUCCESS

SqlClient's built-in AAD auth works perfectly with Proxy. This confirmed the server, database, firewall, and AAD admin are all configured correctly.

### Notable
Externally injected tokens (`conn.AccessToken = token` from `az account get-access-token`) fail even with Proxy policy on the same machine. The issue is specific to externally injected tokens, not related to redirect at all.

---

## Critical Bugs / Findings

### 1. `CREATE USER ... FROM EXTERNAL PROVIDER` Uses Wrong SID for Managed Identity

When you run:
```sql
CREATE USER [PipelinesUsageIndentity] FROM EXTERNAL PROVIDER
```

Azure SQL resolves the MI name via Microsoft Graph and stores the **principalId** (object ID) as the SID:
```
SID stored: 3491bd0f-0e78-44df-b761-17e1ed6253a6  (principalId)
```

But MI tokens contain the **clientId** (application ID) in the claims, and SQL Server matches incoming tokens against the SID using the clientId:
```
Token identifies as: d5c2ef7c-cb1b-4be6-b2bd-19ca351aed80  (clientId)
```

This mismatch causes: `"The server principal ... is not able to access the database"`

**Fix:** Create the user with explicit SID from the clientId:
```sql
-- Convert clientId GUID to binary (little-endian byte order)
-- d5c2ef7c-cb1b-4be6-b2bd-19ca351aed80 → 0x7CEFC2D51BCBE64BB2BD19CA351AED80
CREATE USER [PipelinesUsageIndentity]
  WITH SID = 0x7CEFC2D51BCBE64BB2BD19CA351AED80, TYPE = E
```

### 2. TLS Certificate Mismatch with Private Link DNS

Azure SQL gateway presents a certificate for `*.database.windows.net`. When connecting via Private Link DNS (`*.privatelink.database.windows.net`), TLS hostname verification fails:

```
Certificate name mismatch: cert covers *.database.windows.net,
but connection is to *.privatelink.database.windows.net
```

**Workaround options:**
- `TrustServerCertificate=true` (disables cert validation)
- `HostNameInCertificate=*.database.windows.net` (overrides hostname for cert check)

Both Rust and SqlClient handle this identically. SqlClient has no special "privatelink" handling.

### 3. Externally Injected AAD Tokens Fail on Login

Tokens acquired via `az account get-access-token --resource "https://database.windows.net/"` and injected via `conn.AccessToken` / `access_token` consistently fail with `Login failed for user '<token-identified principal>'`, regardless of connection policy (Proxy or Redirect).

This affects both Rust TDS and Microsoft.Data.SqlClient equally. SqlClient's `ActiveDirectoryDefault` auth (which acquires tokens internally via MSAL) works fine.

This warrants deeper investigation — possibly related to token audience format, app registration, or how externally acquired CLI tokens differ from MSAL-acquired tokens.

### 4. MI Token Acquisition: IMDS Works, IDENTITY_ENDPOINT Does Not

In VNet-injected ACI containers:
- `IDENTITY_ENDPOINT` environment variable is **empty**
- `IDENTITY_HEADER` is set but useless without the endpoint
- **IMDS at `169.254.169.254` IS reachable** and returns valid tokens

Libraries like `azure.identity.DefaultAzureCredential` may fail because they try `IDENTITY_ENDPOINT` first and don't fall through to IMDS. The Azure CLI (`az login --identity`) succeeds because it uses MSAL's `ManagedIdentityClient` which auto-detects IMDS.

**Reliable token acquisition from ACI:**
```python
import urllib.request, json

url = (
    "http://169.254.169.254/metadata/identity/oauth2/token"
    "?api-version=2018-02-01"
    "&resource=https://database.windows.net/"
    "&client_id=<MI_CLIENT_ID>"
)
req = urllib.request.Request(url, headers={"Metadata": "true"})
resp = urllib.request.urlopen(req, timeout=10)
token = json.loads(resp.read())["access_token"]
```

---

## How This Should Actually Work (Correct Procedure)

### Step 1: Infrastructure
    
1. Create Azure SQL Server (AAD-only auth) + database
2. Create a VNet with two subnets: one for private endpoints, one for ACI (delegated to `Microsoft.ContainerInstance/containerGroups`)
3. Create a Private Endpoint for the SQL Server in the PE subnet
4. Create Private DNS Zone `privatelink.database.windows.net`, link to VNet, add zone group to PE
5. Create a User-Assigned Managed Identity

### Step 2: Database Permissions

Grant the MI access using its **clientId** as the SID, not via `FROM EXTERNAL PROVIDER`:

```sql
-- Get the clientId of your MI from Azure portal or:
-- az identity show -g <rg> -n <mi-name> --query clientId -o tsv

-- Convert the clientId GUID to binary little-endian for the SID:
CREATE USER [MyManagedIdentity]
  WITH SID = 0x<clientId-as-binary-le>, TYPE = E;

ALTER ROLE db_datareader ADD MEMBER [MyManagedIdentity];
ALTER ROLE db_datawriter ADD MEMBER [MyManagedIdentity];
```

To convert a GUID to the binary SID format:
```python
import uuid
sid_hex = uuid.UUID('your-client-id-guid').bytes_le.hex().upper()
# Use as: SID = 0x{sid_hex}
```

### Step 3: Connection Policy

Choose based on your scenario:

| Scenario | Policy | Notes |
|---|---|---|
| Simple testing | **Proxy** | No redirect complexity, all traffic through gateway on 1433 |
| Production perf | **Redirect** | Lower latency (direct to backend after initial handshake) |
| Debug redirect | **Default** | Redirect within Azure, proxy from outside |

Set via: `az sql server conn-policy update -g <rg> -s <server> --connection-type <Proxy|Redirect|Default>`

### Step 4: Token Acquisition in Container

Acquire token via IMDS (most reliable in ACI with VNet injection):

```python
import urllib.request, json

def get_mi_token(client_id, resource="https://database.windows.net/"):
    url = (
        f"http://169.254.169.254/metadata/identity/oauth2/token"
        f"?api-version=2018-02-01&resource={resource}&client_id={client_id}"
    )
    req = urllib.request.Request(url, headers={"Metadata": "true"})
    resp = urllib.request.urlopen(req, timeout=10)
    return json.loads(resp.read())["access_token"]
```

### Step 5: Connect

```python
conn = PyCoreConnection({
    "server": "myserver.privatelink.database.windows.net",
    "database": "mydb",
    "access_token": token,
    "trust_server_certificate": True,   # or use host_name_in_cert
    "encryption": "Mandatory",
})
```

### Step 6: Verify

```sql
SELECT @@SERVERNAME, DB_NAME(), SUSER_NAME(),
       CONNECTIONPROPERTY('net_transport'),
       CONNECTIONPROPERTY('client_net_address')
FROM sys.dm_exec_sessions WHERE session_id = @@SPID
```

---

## Open Questions

1. **Why do externally injected user tokens fail?** Both `az account get-access-token` tokens and IMDS MI tokens use audience `https://database.windows.net/`, but only MI tokens work. Is the CLI app registration (`04b07795-8ddb-461a-bbee-02f9e1bf7b46`) being blocked?

2. **Why does redirect fail with user tokens but succeed with MI tokens?** MI on redirect got past TDS auth (18456) to an authorization error, while user tokens failed at TDS auth level on the redirected node.

3. **Is `CREATE USER ... FROM EXTERNAL PROVIDER` SID behavior a bug?** It stores principalId but token matching uses clientId. This silently creates a user that can never authenticate.

---

## Commands Reference

```bash
# Switch connection policy
az sql server conn-policy update -g rust-lib-rg -s mssqlrustlibtest --connection-type Proxy

# Check connection policy
az sql server conn-policy show -g rust-lib-rg -s mssqlrustlibtest

# Build + deploy ACI
cd mssql-py-core/aci-test
az acr build --registry tdslibrs --image aci-rust-sql-test:cc18 . --no-logs
az container delete -g rust-lib-rg -n aci-rust-sql-test -y
az container create -g rust-lib-rg -n aci-rust-sql-test \
  --image tdslibrs.azurecr.io/aci-rust-sql-test:cc18 \
  --os-type Linux --location westus3 \
  --acr-identity "<MI-resource-id>" \
  --assign-identity "<MI-resource-id>" \
  --cpu 1 --memory 1.5 \
  --restart-policy Never \
  --subnet "<ACI-subnet-id>"

# Check logs
az container logs -g rust-lib-rg -n aci-rust-sql-test

# Convert GUID to SID hex
python3 -c "import uuid; print('0x' + uuid.UUID('d5c2ef7c-cb1b-4be6-b2bd-19ca351aed80').bytes_le.hex().upper())"
```

---

## Automated Pipeline Smoke Tests

The `PrivateLink_Smoke` stage in `validation-pipeline.yml` automates this flow for every non-PR build:

1. Downloads the mssql-py-core Linux x64 wheel from the Build stage
2. Builds a Docker image with the wheel + pytest smoke suite (`Dockerfile.smoke`)
3. Pushes to ACR as `tdslibrs.azurecr.io/pycore-smoke:smoke-<BuildId>-<timestamp>`
4. Creates an ACI instance on the trusted VNet (`default2` subnet) with user-assigned MI
5. pytest runs smoke tests, writes JUnit XML to `/results/`
6. Pipeline downloads results, publishes test run, tears down ACI + ACR image

### Auth Modes

Controlled by `SMOKE_AUTH_MODE` env var:

- **`managed_identity`** (default in ACI) — IMDS token acquisition
- **`sql_auth`** — `user_name`/`password` for local testing
- **`access_token`** — pre-fetched token via `ACCESS_TOKEN` env var

### Local Usage

```bash
export SMOKE_AUTH_MODE=sql_auth
export SQL_SERVER=localhost
export SQL_PASSWORD=YourPassword
cd mssql-py-core/aci-test
pytest smoke/ -v
```

### Files

| Path | Purpose |
|------|---------|
| `mssql-py-core/aci-test/smoke/conftest.py` | Auth fixtures |
| `mssql-py-core/aci-test/smoke/test_smoke.py` | Smoke test cases |
| `mssql-py-core/aci-test/Dockerfile.smoke` | ACI container image |
| `.pipeline/templates/private-link-smoke-template.yml` | Pipeline template |
| `.pipeline/scripts/cleanup-private-link.sh` | Stale resource cleanup |
