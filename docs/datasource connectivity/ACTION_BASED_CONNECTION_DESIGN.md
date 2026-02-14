# Action-Based Connection Design for ParsedDataSource

## Problem Statement

Currently, `ParsedDataSource` is a passive data structure. Consumers must implement complex conditional logic to determine what actions to take:

```rust
// Current approach - consumer has too much logic
let parsed = ParsedDataSource::parse(datasource, false)?;

if needs_ssrp_query(&parsed) {
    // Query SQL Browser
    let port = query_ssrp(...)?;
    if can_use_cache {
        // Update cache
    }
}

match parsed.get_protocol_type() {
    ProtocolType::Tcp => { /* connect via TCP */ }
    ProtocolType::NamedPipe => { /* connect via Named Pipe */ }
    // ... more protocol handling
}

if parallel_connect {
    // Handle multi-subnet failover
}
```

**Issues:**
1. Logic scattered across multiple consumer methods
2. Conditional checks repeated in different places
3. Easy to miss edge cases or wrong ordering
4. Hard to test connection logic comprehensively
5. No clear separation between parsing and connection strategy

## Proposed Solution: Action Chain Pattern

Convert `ParsedDataSource` into an ordered sequence of **Connection Actions** that represent the steps needed to establish a connection. Consumers simply execute the action chain.

### Core Design

```rust
/// Represents a single action in the connection process
#[derive(Debug, Clone)]
pub enum ConnectionAction {
    /// Check connection cache for previously resolved connection info
    CheckCache {
        cache_key: String,
    },
    
    /// Query SQL Server Browser (SSRP) to resolve instance port/details
    QuerySsrp {
        server: String,
        instance: String,
        /// Where to store the result (for next action)
        result_slot: ResultSlot,
    },
    
    /// Update connection cache with resolved information
    UpdateCache {
        cache_key: String,
        connection_info: Box<ConnectionAction>, // The resolved connection
    },
    
    /// Attempt TCP connection
    ConnectTcp {
        host: String,
        port: u16,
        timeout: Duration,
    },
    
    /// Attempt Named Pipe connection
    ConnectNamedPipe {
        pipe_path: String,
        timeout: Duration,
    },
    
    /// Attempt Shared Memory connection (Windows only)
    #[cfg(windows)]
    ConnectSharedMemory {
        instance_name: String,
        timeout: Duration,
    },
    
    /// Attempt Dedicated Admin Connection (DAC)
    ConnectDac {
        host: String,
        timeout: Duration,
    },
    
    /// Resolve LocalDB instance to Named Pipe path (Windows only)
    #[cfg(windows)]
    ResolveLocalDb {
        instance_name: String,
        result_slot: ResultSlot,
    },
    
    /// Try multiple connection actions in sequence (failover)
    TrySequence {
        actions: Vec<ConnectionAction>,
        /// Stop on first success or continue through all
        fail_fast: bool,
    },
    
    /// Try multiple connection actions in parallel (MultiSubnetFailover)
    TryParallel {
        actions: Vec<ConnectionAction>,
        /// How many can fail before giving up
        min_successes: usize,
    },
}

/// Slot for storing intermediate results between actions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResultSlot {
    /// Port resolved from SSRP
    ResolvedPort,
    /// Pipe path resolved from LocalDB
    ResolvedPipePath,
    /// Full connection info from cache
    CachedConnectionInfo,
}

/// Result of executing an action
#[derive(Debug)]
pub enum ActionResult {
    /// Action completed successfully
    Success(ActionOutcome),
    /// Action failed but process can continue
    Continue(String),
    /// Action failed and process should stop
    Failed(Error),
}

/// Outcome data from successful actions
#[derive(Debug, Clone)]
pub enum ActionOutcome {
    /// Cache hit with connection details
    CacheHit {
        protocol: ProtocolType,
        connection_string: String,
    },
    /// Cache miss
    CacheMiss,
    /// SSRP resolved port
    SsrpResolved {
        port: u16,
    },
    /// LocalDB resolved to pipe path
    LocalDbResolved {
        pipe_path: String,
    },
    /// Connection established
    Connected {
        transport: Box<dyn Transport>,
    },
    /// No action needed
    NoOp,
}
```

### Connection Strategy Builder

The `ParsedDataSource` now generates an action chain:

```rust
impl ParsedDataSource {
    /// Generate the connection action chain for this data source
    pub fn to_connection_actions(&self) -> ConnectionActionChain {
        let mut builder = ConnectionActionChainBuilder::new();
        
        // Step 1: Check cache if applicable
        if self.can_use_cache {
            builder.add_check_cache(&self.alias);
        }
        
        // Step 2: Handle LocalDB resolution (Windows only)
        #[cfg(windows)]
        if self.protocol_name == "localdb" {
            builder.add_resolve_localdb(&self.instance_name);
            builder.add_connect_named_pipe_from_slot(ResultSlot::ResolvedPipePath);
            return builder.build();
        }
        
        // Step 3: Handle SSRP query if needed
        if self.needs_ssrp() {
            builder.add_ssrp_query(&self.server_name, &self.instance_name);
            
            // After SSRP, update cache if allowed
            if self.can_use_cache {
                builder.add_update_cache(&self.alias);
            }
            
            // Connect using resolved port
            builder.add_connect_tcp_from_slot(&self.server_name, ResultSlot::ResolvedPort);
            return builder.build();
        }
        
        // Step 4: Explicit protocol - single connection attempt
        if !self.protocol_name.is_empty() {
            match self.get_protocol_type() {
                ProtocolType::Tcp => {
                    let port = self.parse_port().unwrap_or(1433);
                    builder.add_connect_tcp(&self.server_name, port);
                }
                ProtocolType::NamedPipe => {
                    let pipe = self.get_named_pipe_path();
                    builder.add_connect_named_pipe(&pipe);
                }
                ProtocolType::SharedMemory => {
                    #[cfg(windows)]
                    builder.add_connect_shared_memory(&self.instance_name);
                }
                ProtocolType::Admin => {
                    builder.add_connect_dac(&self.server_name);
                }
                _ => {}
            }
            return builder.build();
        }
        
        // Step 5: Parallel connect (MultiSubnetFailover)
        if self.parallel_connect {
            // Try TCP to all resolved addresses in parallel
            builder.add_parallel_tcp_connect(&self.server_name, 1433);
            return builder.build();
        }
        
        // Step 6: Auto-detect (protocol waterfall)
        builder.add_protocol_waterfall(&self.server_name, self.is_local());
        builder.build()
    }
}
```

### Connection Action Chain

```rust
/// Ordered sequence of actions to establish a connection
#[derive(Debug)]
pub struct ConnectionActionChain {
    actions: Vec<ConnectionAction>,
    metadata: ConnectionMetadata,
}

#[derive(Debug, Clone)]
pub struct ConnectionMetadata {
    pub source_string: String,
    pub server_name: String,
    pub instance_name: String,
    pub explicit_protocol: bool,
    pub timeout: Duration,
}

impl ConnectionActionChain {
    /// Execute the action chain to establish a connection
    pub async fn execute<E: ConnectionExecutor>(
        &self,
        executor: &mut E,
    ) -> TdsResult<Connection> {
        let mut context = ExecutionContext::new();
        
        for action in &self.actions {
            match executor.execute_action(action, &mut context).await? {
                ActionResult::Success(outcome) => {
                    // Store outcome in context for next action
                    context.store_outcome(outcome);
                    
                    // Check if we have a connection
                    if let ActionOutcome::Connected { transport } = outcome {
                        return Ok(Connection::new(transport));
                    }
                }
                ActionResult::Continue(msg) => {
                    // Log and continue to next action
                    debug!("Action continued: {}", msg);
                }
                ActionResult::Failed(err) => {
                    // Action failed critically
                    return Err(err);
                }
            }
        }
        
        Err(Error::ConnectionFailed(
            "All connection actions exhausted".to_string()
        ))
    }
    
    /// Get a human-readable description of the connection strategy
    pub fn describe(&self) -> String {
        let mut desc = String::new();
        for (i, action) in self.actions.iter().enumerate() {
            desc.push_str(&format!("{}. {}\n", i + 1, action.describe()));
        }
        desc
    }
}

/// Execution context for storing intermediate results
#[derive(Debug, Default)]
pub struct ExecutionContext {
    slots: HashMap<ResultSlot, ActionOutcome>,
    attempts: Vec<(ConnectionAction, Result<ActionOutcome, Error>)>,
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn store_outcome(&mut self, outcome: ActionOutcome) {
        // Store in appropriate slot based on outcome type
        match &outcome {
            ActionOutcome::SsrpResolved { .. } => {
                self.slots.insert(ResultSlot::ResolvedPort, outcome);
            }
            ActionOutcome::LocalDbResolved { .. } => {
                self.slots.insert(ResultSlot::ResolvedPipePath, outcome);
            }
            ActionOutcome::CacheHit { .. } => {
                self.slots.insert(ResultSlot::CachedConnectionInfo, outcome);
            }
            _ => {}
        }
    }
    
    pub fn get_outcome(&self, slot: ResultSlot) -> Option<&ActionOutcome> {
        self.slots.get(&slot)
    }
}
```

### Connection Executor Trait

```rust
/// Trait for executing connection actions
/// 
/// This allows different implementations for production, testing, fuzzing, etc.
#[async_trait]
pub trait ConnectionExecutor {
    /// Execute a single connection action
    async fn execute_action(
        &mut self,
        action: &ConnectionAction,
        context: &mut ExecutionContext,
    ) -> TdsResult<ActionResult>;
    
    /// Check connection cache
    async fn check_cache(&self, key: &str) -> Option<CachedConnectionInfo>;
    
    /// Query SQL Server Browser (SSRP)
    async fn query_ssrp(&self, server: &str, instance: &str) -> TdsResult<SsrpResponse>;
    
    /// Update connection cache
    async fn update_cache(&mut self, key: &str, info: CachedConnectionInfo) -> TdsResult<()>;
    
    /// Attempt TCP connection
    async fn connect_tcp(&self, host: &str, port: u16, timeout: Duration) 
        -> TdsResult<Box<dyn Transport>>;
    
    /// Attempt Named Pipe connection
    async fn connect_named_pipe(&self, pipe: &str, timeout: Duration) 
        -> TdsResult<Box<dyn Transport>>;
    
    // ... other connection methods
}

/// Production executor implementation
pub struct ProductionExecutor {
    cache: ConnectionCache,
    ssrp_client: SsrpClient,
    // ... other components
}

#[async_trait]
impl ConnectionExecutor for ProductionExecutor {
    async fn execute_action(
        &mut self,
        action: &ConnectionAction,
        context: &mut ExecutionContext,
    ) -> TdsResult<ActionResult> {
        match action {
            ConnectionAction::CheckCache { cache_key } => {
                if let Some(cached) = self.check_cache(cache_key).await {
                    Ok(ActionResult::Success(ActionOutcome::CacheHit {
                        protocol: cached.protocol,
                        connection_string: cached.connection_string,
                    }))
                } else {
                    Ok(ActionResult::Success(ActionOutcome::CacheMiss))
                }
            }
            
            ConnectionAction::QuerySsrp { server, instance, .. } => {
                match self.query_ssrp(server, instance).await {
                    Ok(response) => Ok(ActionResult::Success(
                        ActionOutcome::SsrpResolved { port: response.port }
                    )),
                    Err(e) => Ok(ActionResult::Continue(
                        format!("SSRP query failed: {}", e)
                    )),
                }
            }
            
            ConnectionAction::ConnectTcp { host, port, timeout } => {
                match self.connect_tcp(host, *port, *timeout).await {
                    Ok(transport) => Ok(ActionResult::Success(
                        ActionOutcome::Connected { transport }
                    )),
                    Err(e) => Ok(ActionResult::Continue(
                        format!("TCP connection failed: {}", e)
                    )),
                }
            }
            
            ConnectionAction::TrySequence { actions, fail_fast } => {
                for action in actions {
                    match self.execute_action(action, context).await? {
                        ActionResult::Success(outcome) => {
                            return Ok(ActionResult::Success(outcome));
                        }
                        ActionResult::Continue(msg) if !fail_fast => {
                            debug!("Continuing sequence after: {}", msg);
                            continue;
                        }
                        result => return Ok(result),
                    }
                }
                Ok(ActionResult::Continue("All sequence actions failed".to_string()))
            }
            
            // ... other action implementations
            _ => todo!("Implement remaining actions"),
        }
    }
    
    // ... implement other trait methods
}
```

## Example Usage

### Simple TCP Connection

```rust
// Input: "tcp:myserver,1433"
let parsed = ParsedDataSource::parse("tcp:myserver,1433", false)?;
let action_chain = parsed.to_connection_actions();

// Generated chain:
// 1. ConnectTcp { host: "myserver", port: 1433, timeout: 15s }

let mut executor = ProductionExecutor::new();
let connection = action_chain.execute(&mut executor).await?;
```

### Named Instance with SSRP

```rust
// Input: "myserver\SQLEXPRESS"
let parsed = ParsedDataSource::parse("myserver\\SQLEXPRESS", false)?;
let action_chain = parsed.to_connection_actions();

// Generated chain:
// 1. CheckCache { cache_key: "myserver\SQLEXPRESS" }
// 2. QuerySsrp { server: "myserver", instance: "SQLEXPRESS", result_slot: ResolvedPort }
// 3. UpdateCache { cache_key: "myserver\SQLEXPRESS", ... }
// 4. ConnectTcp { host: "myserver", port: <from_slot>, timeout: 15s }

let mut executor = ProductionExecutor::new();
let connection = action_chain.execute(&mut executor).await?;
```

### Protocol Waterfall (Auto-detect)

```rust
// Input: "myserver" (no protocol specified)
let parsed = ParsedDataSource::parse("myserver", false)?;
let action_chain = parsed.to_connection_actions();

// Generated chain:
// 1. TrySequence {
//      actions: [
//        ConnectSharedMemory { instance_name: "MSSQLSERVER" },  // Windows only
//        ConnectTcp { host: "myserver", port: 1433 },
//        ConnectNamedPipe { pipe_path: r"\\myserver\pipe\sql\query" },  // Windows only
//      ],
//      fail_fast: false
//    }

let mut executor = ProductionExecutor::new();
let connection = action_chain.execute(&mut executor).await?;
```

### Parallel Connect (MultiSubnetFailover)

```rust
// Input: "myserver,1433" with MultiSubnetFailover=true
let parsed = ParsedDataSource::parse("myserver,1433", true)?;
let action_chain = parsed.to_connection_actions();

// Generated chain:
// 1. TryParallel {
//      actions: [
//        ConnectTcp { host: "192.168.1.100", port: 1433 },  // IPv4
//        ConnectTcp { host: "2001:db8::1", port: 1433 },    // IPv6
//      ],
//      min_successes: 1
//    }

let mut executor = ProductionExecutor::new();
let connection = action_chain.execute(&mut executor).await?;
```

## Benefits

### 1. **Separation of Concerns**
- **Parsing**: `ParsedDataSource::parse()` - validates syntax
- **Strategy**: `to_connection_actions()` - determines what to do
- **Execution**: `ConnectionExecutor` - performs the actions

### 2. **Clear Connection Logic**
- Action chain explicitly shows the connection strategy
- Easy to understand what will happen
- `describe()` method provides human-readable explanation

### 3. **Testability**
```rust
// Test connection strategy without actual networking
let parsed = ParsedDataSource::parse("myserver\\INST1", false)?;
let actions = parsed.to_connection_actions();

assert_eq!(actions.len(), 4);
assert!(matches!(actions[0], ConnectionAction::CheckCache { .. }));
assert!(matches!(actions[1], ConnectionAction::QuerySsrp { .. }));
```

### 4. **Mock Executor for Testing**
```rust
pub struct MockExecutor {
    ssrp_responses: HashMap<String, u16>,
    cache: HashMap<String, CachedConnectionInfo>,
}

impl MockExecutor {
    pub fn with_ssrp_response(mut self, instance: &str, port: u16) -> Self {
        self.ssrp_responses.insert(instance.to_string(), port);
        self
    }
}
```

### 5. **Fuzzing Support**
```rust
// Fuzz the action chain execution
let executor = FuzzingExecutor::new(fuzzer_state);
let _ = action_chain.execute(&mut executor).await;
```

### 6. **Extensibility**
- Add new actions (e.g., `ResolveAlwaysOn`, `ConnectReadIntent`)
- Compose complex strategies
- No changes to consumer code

### 7. **Observable Connection Process**
```rust
// Log each action for debugging
for action in action_chain.actions() {
    log::debug!("Will attempt: {}", action.describe());
}

// Inspect execution history
let result = action_chain.execute(&mut executor).await;
for (action, outcome) in executor.execution_history() {
    log::info!("{} => {:?}", action.describe(), outcome);
}
```

## Migration Path

### Phase 1: Add Action Types (Non-breaking)
- Define `ConnectionAction` enum
- Add `to_connection_actions()` method to `ParsedDataSource`
- Keep existing methods (`needs_ssrp()`, `can_use_cache`, etc.)

### Phase 2: Implement Executor
- Create `ConnectionExecutor` trait
- Implement `ProductionExecutor`
- Move connection logic from `TdsConnectionProvider` to executor

### Phase 3: Update Consumers
- Replace conditional logic with `action_chain.execute()`
- Remove old helper methods

### Phase 4: Deprecate Old API
- Mark old methods as deprecated
- Eventually remove

## Alternative Designs Considered

### Builder Pattern
```rust
ConnectionBuilder::new()
    .with_cache_check()
    .with_ssrp_query()
    .with_tcp_connect()
    .build()
```
**Rejected**: Requires manual building, loses the benefit of automatic strategy generation.

### Strategy Pattern with Enums
```rust
enum ConnectionStrategy {
    DirectTcp,
    SsrpThenTcp,
    ProtocolWaterfall,
    ParallelConnect,
}
```
**Rejected**: Limited to predefined strategies, not composable.

### State Machine
```rust
enum ConnectionState {
    CheckingCache,
    QueryingSsrp,
    Connecting,
    Connected,
}
```
**Rejected**: Complex state transitions, harder to test individual steps.

## Conclusion

The Action Chain pattern provides:
- ✅ Clear separation between parsing and connection logic
- ✅ Explicit, inspectable connection strategies  
- ✅ Easy to test, mock, and fuzz
- ✅ Extensible for new connection scenarios
- ✅ Maintains compatibility during migration

This design transforms `ParsedDataSource` from a passive data structure into an active strategy generator, moving conditional complexity from consumers into a well-structured, testable action framework.
