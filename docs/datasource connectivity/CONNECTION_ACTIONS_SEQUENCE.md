# Connection Actions Sequence Diagram

This document explains how the connection action chain works in `mssql-tds`.

## Overview

The connection flow uses an **Action Chain Pattern** to convert a datasource string into a sequence of executable actions, providing explicit and testable connection strategies.

## High-Level Flow

::: mermaid
flowchart LR
    A[Application] -->|"tcp:server,1433"| B[DatasourceParser]
    B -->|ParsedDataSource| C[ConnectionActionChain]
    C -->|TransportContext| D[TdsClient<br/>Connected]
:::

## Detailed Sequence Diagram

::: mermaid
sequenceDiagram
    autonumber
    participant User
    participant Provider as TdsConnectionProvider
    participant Parser as DatasourceParser
    participant Chain as ConnectionActionChain
    participant Transport as NetworkTransport
    participant SQL as SQL Server

    User->>Provider: create_client(context, "tcp:server,1433")
    
    Provider->>Parser: parse(datasource)
    
    Note over Parser: 1. Parse protocol prefix "tcp:"<br/>2. Parse server "server"<br/>3. Parse port "1433"<br/>4. Check cache eligibility
    
    Parser-->>Provider: ParsedDataSource
    
    Provider->>Chain: to_connection_actions(timeout_ms)
    
    Note over Chain: Decide action chain based on:<br/><br/>IF explicit protocol (tcp:)<br/>  → [ConnectTcp{server, 1433}]<br/><br/>IF named instance (server\inst)<br/>  → [CheckCache, QuerySsrp,<br/>     UpdateCache, ConnectTcpSlot]<br/><br/>IF no protocol (just "server")<br/>  → [CheckCache, TrySequence[<br/>     SharedMem, TCP, NamedPipe]]
    
    Chain-->>Provider: ConnectionActionChain
    
    Provider->>Chain: resolve_transport_contexts()
    Chain-->>Provider: Vec of TransportContext
    
    loop For each transport_context
        Provider->>Transport: create_transport()
        Transport->>SQL: TCP Connect
        Transport->>SQL: TLS Handshake
        SQL-->>Transport: TLS Established
        Transport->>SQL: TDS Login
        SQL-->>Transport: Login Response
        Transport-->>Provider: TdsTransport
    end
    
    Provider-->>User: TdsClient (Connected)
:::

## Connection Action Types

| Action | Purpose | When Used |
|--------|---------|-----------|
| `CheckCache` | Check if we have cached connection info | Before SSRP for named instances |
| `QuerySsrp` | Query SQL Browser for instance port | Named instance without explicit port |
| `UpdateCache` | Cache the resolved port | After successful SSRP |
| `ConnectTcp` | Direct TCP connection | Explicit `tcp:` or port specified |
| `ConnectTcpFromSlot` | TCP using resolved port | After SSRP resolves port |
| `ConnectNamedPipe` | Named Pipe connection | Explicit `np:` protocol |
| `ConnectSharedMemory` | Shared Memory (Windows) | Local connections, `lpc:` |
| `ConnectDac` | Dedicated Admin Connection | `admin:` protocol |
| `TrySequence` | Try protocols in order | No explicit protocol |
| `TryParallel` | Try addresses in parallel | MultiSubnetFailover=true |
| `ResolveLocalDb` | Resolve LocalDB instance | `(localdb)\Instance` |

## Decision Tree for Action Chain Generation

::: mermaid
flowchart TD
    A[ParsedDataSource<br/>to_connection_actions] --> B{What type<br/>of connection?}
    
    B -->|Explicit Protocol<br/>tcp:, np:, lpc:, admin:| C[Single Action]
    B -->|Named Instance<br/>server\instance| D[SSRP Resolution Chain]
    B -->|No Protocol<br/>just 'server'| E[Protocol Waterfall]
    
    C --> C1[ConnectTcp]
    C --> C2[ConnectNamedPipe]
    C --> C3[ConnectSharedMemory]
    C --> C4[ConnectDac]
    
    D --> D1[1. CheckCache]
    D1 --> D2[2. QuerySsrp]
    D2 --> D3[3. UpdateCache]
    D3 --> D4[4. ConnectTcpFromSlot]
    
    E --> E1[1. CheckCache]
    E1 --> E2[2. TrySequence]
    E2 --> E3[SharedMemory<br/>Windows only]
    E2 --> E4[TCP]
    E2 --> E5[NamedPipe]
:::

## Example Action Chains

### 1. Explicit TCP with Port: `"tcp:myserver,1433"`

::: mermaid
flowchart LR
    subgraph ActionChain
        A[ConnectTcp<br/>host: myserver<br/>port: 1433]
    end
    
    Input["tcp:myserver,1433"] --> ActionChain
    ActionChain --> Output[TdsClient]
:::

```rust
ConnectionActionChain {
    actions: [
        ConnectTcp { host: "myserver", port: 1433, timeout_ms: 15000 }
    ],
    metadata: { explicit_protocol: true }
}
```

### 2. Named Instance: `"myserver\SQLEXPRESS"`

::: mermaid
flowchart LR
    subgraph ActionChain
        A[CheckCache] --> B[QuerySsrp]
        B --> C[UpdateCache]
        C --> D[ConnectTcpFromSlot]
    end
    
    Input["myserver\SQLEXPRESS"] --> ActionChain
    ActionChain --> Output[TdsClient]
:::

```rust
ConnectionActionChain {
    actions: [
        CheckCache { cache_key: "myserver\\SQLEXPRESS" },
        QuerySsrp { server: "myserver", instance: "SQLEXPRESS", result_slot: ResolvedPort },
        UpdateCache { cache_key: "myserver\\SQLEXPRESS", port: 0 },
        ConnectTcpFromSlot { host: "myserver", port_slot: ResolvedPort, timeout_ms: 15000 }
    ],
    metadata: { explicit_protocol: false }
}
```

### 3. No Protocol (Waterfall): `"myserver"`

::: mermaid
flowchart LR
    subgraph ActionChain
        A[CheckCache] --> B[TrySequence]
        subgraph TrySequence[TrySequence - fail_fast: true]
            B1[SharedMemory<br/>Windows only]
            B2[ConnectTcp<br/>port: 1433]
            B3[ConnectNamedPipe]
        end
        B --> B1
        B --> B2
        B --> B3
    end
    
    Input["myserver"] --> ActionChain
    ActionChain --> Output[TdsClient]
:::

```rust
ConnectionActionChain {
    actions: [
        CheckCache { cache_key: "myserver" },
        TrySequence {
            actions: [
                ConnectSharedMemory { instance: "MSSQLSERVER", timeout_ms: 15000 },
                ConnectTcp { host: "myserver", port: 1433, timeout_ms: 15000 },
                ConnectNamedPipe { pipe: "\\\\myserver\\pipe\\sql\\query", timeout_ms: 15000 }
            ],
            fail_fast: true
        }
    ],
    metadata: { explicit_protocol: false }
}
```

### 4. LocalDB (Windows): `"(localdb)\MSSQLLocalDB"`

::: mermaid
flowchart LR
    subgraph ActionChain
        A[ResolveLocalDb<br/>instance: MSSQLLocalDB] --> B[ConnectNamedPipeFromSlot]
    end
    
    Input["(localdb)\MSSQLLocalDB"] --> ActionChain
    ActionChain --> Output[TdsClient]
:::

```rust
ConnectionActionChain {
    actions: [
        ResolveLocalDb { instance_name: "MSSQLLocalDB", result_slot: ResolvedPipePath },
        ConnectNamedPipeFromSlot { path_slot: ResolvedPipePath, timeout_ms: 15000 }
    ],
    metadata: { explicit_protocol: false }
}
```

## ExecutionContext and Result Slots

The `ExecutionContext` stores intermediate results between actions:

::: mermaid
classDiagram
    class ExecutionContext {
        +HashMap~ResultSlot, ActionOutcome~ slots
        +Vec~String, Result~ attempts
        +store_outcome(outcome)
        +get_outcome(slot) ActionOutcome
        +get_port(slot) u16
        +get_pipe_path(slot) String
    }
    
    class ResultSlot {
        <<enumeration>>
        ResolvedPort
        ResolvedPipePath
        CachedConnectionInfo
    }
    
    class ActionOutcome {
        <<enumeration>>
        CacheHit
        CacheMiss
        SsrpResolved
        LocalDbResolved
        Connected
        CacheUpdated
    }
    
    ExecutionContext --> ResultSlot : uses
    ExecutionContext --> ActionOutcome : stores
:::

### Slot Usage Flow

::: mermaid
sequenceDiagram
    participant SSRP as QuerySsrp Action
    participant Ctx as ExecutionContext
    participant TCP as ConnectTcpFromSlot

    SSRP->>Ctx: store_outcome(SsrpResolved { port: 49721 })
    Note over Ctx: slots[ResolvedPort] = SsrpResolved { port: 49721 }
    
    TCP->>Ctx: get_port(ResolvedPort)
    Ctx-->>TCP: Some(49721)
    
    Note over TCP: Connect to server:49721
:::

## Key Files

- [datasource_parser.rs](../mssql-tds/src/connection/datasource_parser.rs) - Parses datasource string, generates action chain
- [connection_actions.rs](../mssql-tds/src/connection/connection_actions.rs) - Action types and chain execution
- [tds_connection_provider.rs](../mssql-tds/src/connection_provider/tds_connection_provider.rs) - Executes action chain, creates client
