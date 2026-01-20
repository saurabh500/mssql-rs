# Connection Cache Implementation Specification

This document defines the detailed specification for implementing the connection cache in `mssql-tds`. The cache optimizes subsequent connections by storing previously resolved connection information, avoiding redundant SSRP (SQL Server Browser) queries.

## Overview

The connection cache (analogous to ODBC's **LastConnectCache**) is a process-wide singleton that stores successful connection resolution results. When connecting to a named SQL Server instance, the driver must query SQL Server Browser (SSRP) to discover the instance's port. The cache eliminates this round-trip for repeat connections.

::: mermaid
flowchart LR
    subgraph FirstConnection["First Connection to server\instance"]
        A1[CheckCache] --> A2[CacheMiss]
        A2 --> A3[QuerySsrp]
        A3 --> A4[UpdateCache]
        A4 --> A5[ConnectTcp:49721]
    end
    
    subgraph SecondConnection["Subsequent Connection"]
        B1[CheckCache] --> B2[CacheHit: port 49721]
        B2 --> B3[ConnectTcp:49721]
    end
    
    FirstConnection -.->|"Cache populated"| SecondConnection
:::

## Goals

1. **Reduce latency** - Eliminate SSRP UDP round-trip on repeat connections
2. **Thread-safe** - Support concurrent access from multiple async tasks
3. **Memory-bounded** - Limit cache size to prevent unbounded growth
4. **Configurable** - Allow TTL and size limits via configuration
5. **Observable** - Provide metrics/logging for cache hits/misses

## Non-Goals

1. **Persistence** - Cache is in-memory only, not persisted across process restarts
2. **Distributed caching** - No cross-process or cross-machine sharing
3. **Connection pooling** - This cache stores *resolution info*, not actual connections

## Data Model

### Cache Key

The cache key is the **alias** derived from the original connection string:

```
server\instance  →  "server\instance"
SERVER\Instance  →  "server\instance"  (case-insensitive, normalized to lowercase)
myserver         →  "myserver"          (default instance)
```

**Key Format:** `{server_name}\{instance_name}` (normalized to lowercase)

### Cache Value

```rust
/// Cached connection resolution information
#[derive(Debug, Clone)]
pub struct CachedConnectionInfo {
    /// The protocol that was successfully used
    pub protocol: ProtocolType,
    
    /// Resolved port number (for TCP connections)
    pub port: u16,
    
    /// Resolved pipe path (for Named Pipe connections)
    pub pipe_path: Option<String>,
    
    /// Timestamp when this entry was cached
    pub cached_at: Instant,
    
    /// Number of times this entry was used (for metrics)
    pub hit_count: u64,
}
```

### Cache Entry Lifecycle

::: mermaid
stateDiagram-v2
    [*] --> Empty: Process Start
    Empty --> Cached: SSRP Success + UpdateCache
    Cached --> Cached: Cache Hit (increment hit_count)
    Cached --> Evicted: TTL Expired
    Cached --> Evicted: LRU Eviction (capacity reached)
    Cached --> Invalid: Connection Failure
    Evicted --> [*]
    Invalid --> Empty: Entry Removed
:::

## API Design

### Core Types

```rust
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use lru::LruCache;

/// Configuration for the connection cache
#[derive(Debug, Clone)]
pub struct ConnectionCacheConfig {
    /// Maximum number of entries in the cache
    pub max_entries: usize,
    
    /// Time-to-live for cache entries
    pub ttl: Duration,
    
    /// Whether caching is enabled
    pub enabled: bool,
}

impl Default for ConnectionCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 100,
            ttl: Duration::from_secs(300), // 5 minutes
            enabled: true,
        }
    }
}

/// Thread-safe connection cache
pub struct ConnectionCache {
    config: ConnectionCacheConfig,
    entries: RwLock<LruCache<String, CachedConnectionInfo>>,
    metrics: CacheMetrics,
}

/// Cache metrics for observability
#[derive(Debug, Default)]
pub struct CacheMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub invalidations: AtomicU64,
}
```

### Cache Operations

```rust
impl ConnectionCache {
    /// Create a new cache with the given configuration
    pub fn new(config: ConnectionCacheConfig) -> Self;
    
    /// Create a new cache with default configuration
    pub fn default() -> Self;
    
    /// Look up a cache entry by key
    /// 
    /// Returns `Some(info)` if found and not expired, `None` otherwise.
    /// Expired entries are removed lazily on access.
    pub async fn get(&self, key: &str) -> Option<CachedConnectionInfo>;
    
    /// Insert or update a cache entry
    /// 
    /// If the cache is at capacity, the least-recently-used entry is evicted.
    pub async fn put(&self, key: &str, info: CachedConnectionInfo);
    
    /// Invalidate (remove) a cache entry
    /// 
    /// Called when a cached connection attempt fails, indicating stale data.
    pub async fn invalidate(&self, key: &str);
    
    /// Clear all cache entries
    pub async fn clear(&self);
    
    /// Get current cache statistics
    pub fn metrics(&self) -> &CacheMetrics;
    
    /// Get the number of entries currently in the cache
    pub async fn len(&self) -> usize;
}
```

### Global Cache Singleton

```rust
use once_cell::sync::Lazy;

/// Global connection cache instance
static CONNECTION_CACHE: Lazy<ConnectionCache> = Lazy::new(|| {
    ConnectionCache::new(ConnectionCacheConfig::default())
});

/// Get a reference to the global connection cache
pub fn connection_cache() -> &'static ConnectionCache {
    &CONNECTION_CACHE
}

/// Configure the global connection cache
/// 
/// # Panics
/// Panics if called after the cache has been accessed (lazy initialization).
/// Should only be called during application initialization.
pub fn configure_connection_cache(config: ConnectionCacheConfig) {
    // Note: This requires a different approach since Lazy doesn't support reconfiguration.
    // Consider using OnceLock with explicit initialization or a configurable wrapper.
}
```

## Integration with Action Chain

### CheckCache Action

When the `CheckCache` action is executed:

```rust
async fn execute_check_cache(
    &self,
    cache_key: &str,
    context: &mut ExecutionContext,
) -> ActionResult {
    let cache = connection_cache();
    
    match cache.get(cache_key).await {
        Some(info) => {
            debug!("Cache hit for '{}': port={}", cache_key, info.port);
            context.store_outcome(ActionOutcome::CacheHit {
                protocol: info.protocol,
                port: info.port,
            });
            ActionResult::Success(ActionOutcome::CacheHit {
                protocol: info.protocol,
                port: info.port,
            })
        }
        None => {
            debug!("Cache miss for '{}'", cache_key);
            ActionResult::Continue("Cache miss".to_string())
        }
    }
}
```

### UpdateCache Action

When the `UpdateCache` action is executed:

```rust
async fn execute_update_cache(
    &self,
    cache_key: &str,
    port: u16,
    context: &ExecutionContext,
) -> ActionResult {
    // Get the actual port from SSRP result if port is 0 (placeholder)
    let resolved_port = if port == 0 {
        context.get_port(ResultSlot::ResolvedPort).unwrap_or(1433)
    } else {
        port
    };
    
    let info = CachedConnectionInfo {
        protocol: ProtocolType::Tcp,
        port: resolved_port,
        pipe_path: None,
        cached_at: Instant::now(),
        hit_count: 0,
    };
    
    connection_cache().put(cache_key, info).await;
    debug!("Cache updated for '{}': port={}", cache_key, resolved_port);
    
    ActionResult::Success(ActionOutcome::CacheUpdated)
}
```

### Modified Action Chain Flow

When cache is hit, skip SSRP and use cached port directly:

::: mermaid
flowchart TD
    A[Start] --> B{CheckCache}
    B -->|Hit| C[Use cached port]
    B -->|Miss| D[QuerySsrp]
    D --> E[UpdateCache]
    E --> F[ConnectTcp with resolved port]
    C --> G[ConnectTcp with cached port]
    F --> H[Success?]
    G --> I[Success?]
    H -->|Yes| J[Connected]
    H -->|No| K[Fail]
    I -->|Yes| J
    I -->|No| L[Invalidate Cache]
    L --> D
:::

## Cache Invalidation

### When to Invalidate

1. **Connection failure after cache hit** - The cached port may be stale (instance restarted on different port)
2. **Explicit invalidation** - Application requests cache clear
3. **TTL expiration** - Entry exceeds configured time-to-live

### Retry Logic After Invalidation

```rust
// Pseudo-code for connection with cache invalidation retry
async fn connect_with_cache_retry(
    &self,
    context: &ClientContext,
    cache_key: &str,
) -> TdsResult<TdsClient> {
    let cache = connection_cache();
    
    // First attempt: try cached connection
    if let Some(cached_info) = cache.get(cache_key).await {
        match self.try_connect(context, cached_info.port).await {
            Ok(client) => return Ok(client),
            Err(_) => {
                // Cache was stale, invalidate and retry with SSRP
                cache.invalidate(cache_key).await;
                debug!("Invalidated stale cache entry for '{}'", cache_key);
            }
        }
    }
    
    // Second attempt: query SSRP and update cache
    let ssrp_result = self.query_ssrp(context).await?;
    cache.put(cache_key, CachedConnectionInfo {
        protocol: ProtocolType::Tcp,
        port: ssrp_result.port,
        pipe_path: None,
        cached_at: Instant::now(),
        hit_count: 0,
    }).await;
    
    self.try_connect(context, ssrp_result.port).await
}
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MSSQL_TDS_CACHE_ENABLED` | `true` | Enable/disable connection cache |
| `MSSQL_TDS_CACHE_MAX_ENTRIES` | `100` | Maximum cache entries |
| `MSSQL_TDS_CACHE_TTL_SECS` | `300` | Entry time-to-live in seconds |

### Programmatic Configuration

```rust
// Configure before first use
configure_connection_cache(ConnectionCacheConfig {
    max_entries: 200,
    ttl: Duration::from_secs(600),
    enabled: true,
});
```

## Thread Safety

The cache uses `tokio::sync::RwLock` for concurrent access:

- **Read operations** (`get`): Multiple concurrent readers allowed
- **Write operations** (`put`, `invalidate`, `clear`): Exclusive access required

::: mermaid
sequenceDiagram
    participant T1 as Task 1
    participant T2 as Task 2
    participant Cache as ConnectionCache
    participant Lock as RwLock
    
    T1->>Lock: acquire read lock
    Lock-->>T1: read guard
    T2->>Lock: acquire read lock
    Lock-->>T2: read guard (concurrent OK)
    T1->>Cache: get("server\\inst")
    T2->>Cache: get("other\\inst")
    Cache-->>T1: CacheHit
    Cache-->>T2: CacheMiss
    T1->>Lock: release read lock
    T2->>Lock: release read lock
    
    T2->>Lock: acquire write lock
    Lock-->>T2: write guard (exclusive)
    T2->>Cache: put("other\\inst", info)
    T2->>Lock: release write lock
:::

## Observability

### Logging

```rust
// Cache hit
debug!(target: "mssql_tds::cache", "Cache hit for '{}': port={}", key, port);

// Cache miss
debug!(target: "mssql_tds::cache", "Cache miss for '{}'", key);

// Cache update
info!(target: "mssql_tds::cache", "Cache updated: '{}' -> port {}", key, port);

// Cache invalidation
info!(target: "mssql_tds::cache", "Cache invalidated: '{}'", key);

// Eviction
debug!(target: "mssql_tds::cache", "Cache eviction: '{}' (LRU)", evicted_key);
```

### Metrics

Expose via the `CacheMetrics` struct:

```rust
let metrics = connection_cache().metrics();
println!("Cache stats: hits={}, misses={}, hit_rate={:.2}%",
    metrics.hits.load(Ordering::Relaxed),
    metrics.misses.load(Ordering::Relaxed),
    metrics.hit_rate() * 100.0
);
```

## File Structure

```
mssql-tds/src/connection/
├── mod.rs                      # Add: pub mod connection_cache;
├── connection_cache.rs         # NEW: Cache implementation
├── connection_actions.rs       # Update: Integrate cache operations
├── datasource_parser.rs        # No changes needed
└── execution_context.rs        # No changes needed
```

## Implementation Checklist

- [ ] Create `connection_cache.rs` with `ConnectionCache` struct
- [ ] Add `CachedConnectionInfo` struct with all fields
- [ ] Implement `ConnectionCacheConfig` with defaults
- [ ] Implement cache operations (`get`, `put`, `invalidate`, `clear`)
- [ ] Create global singleton with `once_cell::Lazy`
- [ ] Add `CacheMetrics` for observability
- [ ] Integrate `CheckCache` action execution
- [ ] Integrate `UpdateCache` action execution
- [ ] Add cache invalidation on connection failure
- [ ] Add environment variable configuration
- [ ] Add unit tests for cache operations
- [ ] Add integration tests with action chain
- [ ] Add documentation and examples

## Dependencies

Add to `Cargo.toml`:

```toml
[dependencies]
once_cell = "1.19"
lru = "0.12"
```

## Testing Strategy

### Unit Tests

```rust
#[tokio::test]
async fn test_cache_miss_then_hit() {
    let cache = ConnectionCache::new(ConnectionCacheConfig::default());
    
    // Initial lookup should miss
    assert!(cache.get("server\\instance").await.is_none());
    
    // Insert entry
    cache.put("server\\instance", CachedConnectionInfo {
        protocol: ProtocolType::Tcp,
        port: 49721,
        pipe_path: None,
        cached_at: Instant::now(),
        hit_count: 0,
    }).await;
    
    // Now should hit
    let result = cache.get("server\\instance").await;
    assert!(result.is_some());
    assert_eq!(result.unwrap().port, 49721);
}

#[tokio::test]
async fn test_cache_ttl_expiration() {
    let cache = ConnectionCache::new(ConnectionCacheConfig {
        ttl: Duration::from_millis(100),
        ..Default::default()
    });
    
    cache.put("key", info.clone()).await;
    assert!(cache.get("key").await.is_some());
    
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(cache.get("key").await.is_none()); // Expired
}

#[tokio::test]
async fn test_cache_lru_eviction() {
    let cache = ConnectionCache::new(ConnectionCacheConfig {
        max_entries: 2,
        ..Default::default()
    });
    
    cache.put("a", info_a).await;
    cache.put("b", info_b).await;
    cache.put("c", info_c).await; // Should evict "a"
    
    assert!(cache.get("a").await.is_none()); // Evicted
    assert!(cache.get("b").await.is_some());
    assert!(cache.get("c").await.is_some());
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_action_chain_uses_cache() {
    // Setup: Pre-populate cache
    connection_cache().put("testserver\\SQLEXPRESS", CachedConnectionInfo {
        protocol: ProtocolType::Tcp,
        port: 54321,
        pipe_path: None,
        cached_at: Instant::now(),
        hit_count: 0,
    }).await;
    
    // Parse datasource that would normally need SSRP
    let parsed = ParsedDataSource::parse("testserver\\SQLEXPRESS", false)?;
    let chain = parsed.to_connection_actions(15000);
    
    // Verify CheckCache is first action
    assert!(chain.has_cache_check());
    
    // Execute and verify SSRP was skipped (uses cached port)
    // ... (requires mock executor)
}
```

## Security Considerations

1. **No sensitive data** - Cache only stores protocol/port, never credentials
2. **Process isolation** - Cache is per-process, not shared between processes
3. **No persistence** - Cache cleared on process restart, no disk storage

## Compatibility

- **ODBC parity** - Behavior matches ODBC's LastConnectCache
- **JDBC reference** - Similar to JDBC driver's connection info caching
- **ADO.NET** - SqlClient caches connection pool metadata similarly

## References

- [ODBC Driver Connection String](https://docs.microsoft.com/sql/connect/odbc/dsn-connection-string-attribute)
- [SQL Server Browser Service](https://docs.microsoft.com/sql/tools/configuration-manager/sql-server-browser-service)
- [LRU Cache crate](https://docs.rs/lru/latest/lru/)
- [once_cell crate](https://docs.rs/once_cell/latest/once_cell/)
