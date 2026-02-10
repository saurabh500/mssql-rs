# LocalDB Connection Flow

This diagram shows the complete sequence of operations when establishing a LocalDB connection.

```mermaid
sequenceDiagram
    participant Client as Application
    participant ConnStr as Connection String Parser
    participant LocalDB as LocalDB Module
    participant Registry as Windows Registry
    participant DLL as sqluserinstance.dll
    participant API as LocalDB API
    participant NamedPipe as Named Pipe Transport

    Client->>ConnStr: Connection String with "server=(localdb)\\MSSQLLocalDB"
    ConnStr->>ConnStr: Parse connection string
    ConnStr->>ConnStr: Detect LocalDB pattern: (localdb)\\ prefix
    ConnStr->>Client: Return Transport::LocalDB { instance_name }

    Client->>LocalDB: Connect with instance_name
    
    Note over LocalDB,Registry: DLL Discovery Phase
    LocalDB->>Registry: RegOpenKeyExW(HKLM\SOFTWARE\Microsoft\Microsoft SQL Server Local DB\Installed Versions)
    Registry-->>LocalDB: Registry key handle
    
    loop For each installed version
        LocalDB->>Registry: RegEnumKeyExW(enumerate subkeys)
        Registry-->>LocalDB: Version string (e.g., "15.0", "16.0")
        LocalDB->>LocalDB: Parse major.minor, track latest
    end
    
    LocalDB->>Registry: RegOpenKeyExW(latest version subkey)
    Registry-->>LocalDB: Version-specific key handle
    LocalDB->>Registry: RegQueryValueExW("InstanceAPIPath")
    Registry-->>LocalDB: DLL path string
    LocalDB->>LocalDB: Convert UTF-16 to String
    
    Note over LocalDB,DLL: DLL Loading Phase
    LocalDB->>DLL: LoadLibraryW(registry_path)
    alt DLL loaded successfully
        DLL-->>LocalDB: Library handle
    else DLL not found
        LocalDB->>DLL: Try PATH: "sqluserinstance.dll"
        alt Still not found
            LocalDB->>DLL: Try hardcoded: v150, v160, v140
        end
    end
    
    Note over LocalDB,API: Function Binding Phase
    LocalDB->>DLL: GetProcAddress("LocalDBCreateInstance")
    DLL-->>LocalDB: Function pointer
    LocalDB->>DLL: GetProcAddress("LocalDBStartInstance")
    DLL-->>LocalDB: Function pointer
    LocalDB->>DLL: GetProcAddress("LocalDBGetInstanceInfo")
    DLL-->>LocalDB: Function pointer
    LocalDB->>DLL: GetProcAddress("LocalDBFormatMessage")
    DLL-->>LocalDB: Function pointer
    
    Note over LocalDB,API: Instance Resolution Phase
    LocalDB->>API: LocalDBGetInstanceInfo(instance_name)
    API-->>LocalDB: Instance info (state, pipe name)
    
    alt Instance not running
        LocalDB->>API: LocalDBStartInstance(instance_name)
        API->>API: Start SQL Server process
        API-->>LocalDB: Started, pipe name
    else Instance already running
        LocalDB->>LocalDB: Use existing pipe name
    end
    
    Note over LocalDB,NamedPipe: Named Pipe Connection Phase
    LocalDB->>LocalDB: Extract pipe name (e.g., "np:\\.\pipe\LOCALDB#HASH\tsql\query")
    LocalDB->>NamedPipe: Connect to named pipe
    NamedPipe->>NamedPipe: CreateFileW with pipe path
    NamedPipe-->>LocalDB: Pipe handle
    
    LocalDB-->>Client: TcpStream (wrapped pipe handle)
    
    Note over Client,NamedPipe: Authentication & Query Phase
    Client->>NamedPipe: Send TDS packets (login, queries)
    NamedPipe->>NamedPipe: ReadFile/WriteFile operations
    NamedPipe-->>Client: Response TDS packets
```

## Key Phases

### 1. Connection String Parsing
- Detects `(localdb)\` prefix pattern
- Extracts instance name (e.g., `MSSQLLocalDB`)
- Returns `Transport::LocalDB` variant

### 2. DLL Discovery (Registry-Based)
- Queries Windows Registry: `HKLM\SOFTWARE\Microsoft\Microsoft SQL Server Local DB\Installed Versions`
- Enumerates all installed versions (15.0, 16.0, 17.0, etc.)
- Selects latest version by comparing major.minor numbers
- Reads `InstanceAPIPath` value to get DLL location
- **Fallback**: PATH → Hardcoded paths (v150, v160, v140)

### 3. Function Binding
Loads 4 essential LocalDB API functions:
- `LocalDBCreateInstance` - Create new instances
- `LocalDBStartInstance` - Start stopped instances
- `LocalDBGetInstanceInfo` - Get instance state and pipe name
- `LocalDBFormatMessage` - Format error messages

### 4. Instance Resolution
- Calls `LocalDBGetInstanceInfo` to check instance state
- If not running: calls `LocalDBStartInstance` to launch SQL Server process
- Retrieves named pipe path from instance info

### 5. Named Pipe Connection
- Extracts pipe path (e.g., `\\.\pipe\LOCALDB#62AFEA01\tsql\query`)
- Uses Windows `CreateFileW` to connect to pipe
- Wraps pipe handle as `TcpStream` for compatibility

### 6. Data Transfer
- All TDS protocol communication happens over the named pipe
- Uses standard `ReadFile`/`WriteFile` Windows APIs

## Important Notes

- **LocalDB is essentially a named pipe with automatic instance management**
- The LocalDB API's main job is to:
  1. Resolve instance name → pipe path
  2. Ensure the instance is running
  3. Return the pipe path for connection
- All actual database communication uses standard named pipe I/O
- The implementation is **ODBC-compatible** (uses same registry discovery)
