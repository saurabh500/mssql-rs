# Action-Based Connection Migration Guide

## Overview

This guide helps you migrate from the old conditional-based connection logic to the new action-based design.

## Phase 1: Non-Breaking Addition (COMPLETE)

✅ All new types and methods have been added without breaking existing code:
- `ConnectionAction` enum and related types
- `ConnectionActionChain` and `ConnectionActionChainBuilder`
- `ConnectionExecutor` trait
- `ParsedDataSource::to_connection_actions()` method
- All existing methods remain unchanged and functional

## Current State

The codebase now supports **both** patterns:

### Old Pattern (Still Supported)
```rust
let parsed = ParsedDataSource::parse("myserver\\SQLEXPRESS", false)?;

// Consumer checks various flags
if parsed.needs_ssrp() {
    // Query SQL Browser
    let port = query_ssrp(&parsed.server_name, &parsed.instance_name)?;
    
    if parsed.can_use_cache {
        // Update cache
        cache.update(&parsed.alias, port);
    }
    
    // Connect
    connect_tcp(&parsed.server_name, port)?;
} else {
    // Direct connection logic
}
```

### New Pattern (Available Now)
```rust
let parsed = ParsedDataSource::parse("myserver\\SQLEXPRESS", false)?;
let action_chain = parsed.to_connection_actions(15000);

// Consumer just executes the chain
let connection = executor.execute_chain(action_chain).await?;
```

## Inspecting Connection Strategies

You can now inspect what will happen before executing:

```rust
let parsed = ParsedDataSource::parse("myserver\\SQLEXPRESS", false)?;
let chain = parsed.to_connection_actions(15000);

// Print human-readable description
println!("{}", chain.describe());

// Output:
// Connection strategy for 'myserver\SQLEXPRESS'
// Server: myserver
// Instance: SQLEXPRESS
// Explicit protocol: false
//
// Action sequence:
// 1. Check connection cache for 'myserver\SQLEXPRESS'
// 2. Query SQL Browser for 'myserver\SQLEXPRESS'
// 3. Update cache 'myserver\SQLEXPRESS' with port 0
// 4. Connect via TCP to myserver (port from ResolvedPort)

// Programmatically inspect actions
for action in chain.actions() {
    match action {
        ConnectionAction::QuerySsrp { server, instance, .. } => {
            println!("Will need SSRP query for {}\\{}", server, instance);
        }
        ConnectionAction::ConnectTcp { host, port, .. } => {
            println!("Will connect via TCP to {}:{}", host, port);
        }
        _ => {}
    }
}
```

## Testing with the New Design

### Old Approach (Complex Mocking)
```rust
// Had to mock multiple components and check flags
let parsed = ParsedDataSource::parse("myserver\\INST1", false)?;
assert!(parsed.needs_ssrp());
assert!(parsed.can_use_cache);
// Still need to implement the logic...
```

### New Approach (Test Action Generation)
```rust
let parsed = ParsedDataSource::parse("myserver\\INST1", false)?;
let chain = parsed.to_connection_actions(15000);

// Verify the strategy without any networking
assert_eq!(chain.len(), 4);
assert!(matches!(chain.actions()[0], ConnectionAction::CheckCache { .. }));
assert!(matches!(chain.actions()[1], ConnectionAction::QuerySsrp { .. }));
assert!(matches!(chain.actions()[2], ConnectionAction::UpdateCache { .. }));
assert!(matches!(chain.actions()[3], ConnectionAction::ConnectTcpFromSlot { .. }));
```

## Phase 2: Implement ConnectionExecutor (Next Step)

To complete the migration, implement a `ConnectionExecutor`:

```rust
use async_trait::async_trait;
use mssql_tds::connection::connection_actions::{
    ConnectionExecutor, ActionResult, ActionOutcome, CachedConnectionInfo, SsrpResponse,
    ExecutionContext,
};

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
        // Use the default implementation
        self.execute_action_default(action, context).await
    }

    async fn check_cache(&self, key: &str) -> Option<CachedConnectionInfo> {
        self.cache.get(key)
    }

    async fn query_ssrp(&self, server: &str, instance: &str) -> TdsResult<SsrpResponse> {
        self.ssrp_client.query(server, instance).await
    }

    async fn update_cache(&mut self, key: &str, info: CachedConnectionInfo) -> TdsResult<()> {
        self.cache.insert(key.to_string(), info);
        Ok(())
    }

    async fn connect_tcp(&self, host: &str, port: u16, timeout: Duration) -> TdsResult<()> {
        // Perform actual TCP connection
        // Return Ok(()) on success, Err on failure
        todo!()
    }

    // ... implement other trait methods
}
```

## Phase 3: Update Consumers

Once the executor is implemented, update connection code:

### Before
```rust
async fn connect(datasource: &str) -> TdsResult<Connection> {
    let mut context = ClientContext::with_data_source(datasource);
    let parsed = context.parse_datasource(datasource)?;
    
    // Complex conditional logic
    if Self::needs_ssrp_query(&context) {
        // SSRP logic
        let response = ssrp::query(...).await?;
        
        if parsed.can_use_cache {
            // Cache logic
        }
        
        // Connect with resolved port
    } else if context.explicit_protocol {
        // Direct connection
    } else {
        // Protocol waterfall
        for protocol in protocols {
            if let Ok(conn) = try_connect(protocol) {
                return Ok(conn);
            }
        }
    }
    
    Err(Error::ConnectionFailed(...))
}
```

### After
```rust
async fn connect(datasource: &str) -> TdsResult<Connection> {
    let parsed = ParsedDataSource::parse(datasource, false)?;
    let action_chain = parsed.to_connection_actions(15000);
    
    let mut executor = ProductionExecutor::new();
    let mut context = ExecutionContext::new();
    
    for action in action_chain.actions() {
        match executor.execute_action(action, &mut context).await? {
            ActionResult::Success(ActionOutcome::Connected) => {
                return Ok(create_connection_from_context(context));
            }
            ActionResult::Continue(msg) => {
                debug!("Action continued: {}", msg);
            }
            ActionResult::Failed(err) => {
                return Err(err);
            }
            ActionResult::Success(outcome) => {
                context.store_outcome(outcome);
            }
        }
    }
    
    Err(Error::ConnectionFailed("All actions exhausted".to_string()))
}
```

## Example: Complete Connection Flow

Here's how a complete connection flow works with the new design:

```rust
use mssql_tds::connection::datasource_parser::ParsedDataSource;

async fn example_connection() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Parse the datasource string
    let datasource = "myserver\\SQLEXPRESS";
    let parsed = ParsedDataSource::parse(datasource, false)?;
    
    // 2. Generate action chain with timeout
    let chain = parsed.to_connection_actions(15000);
    
    // 3. (Optional) Inspect the strategy
    println!("Will execute {} actions:", chain.len());
    for (i, action) in chain.actions().iter().enumerate() {
        println!("  {}. {}", i + 1, action.describe());
    }
    
    // 4. Execute the chain
    let mut executor = ProductionExecutor::new();
    let connection = execute_action_chain(chain, &mut executor).await?;
    
    println!("Connected successfully!");
    Ok(())
}
```

## Benefits of Migration

1. **Separation of Concerns**
   - Parsing: `ParsedDataSource::parse()`
   - Strategy: `to_connection_actions()`
   - Execution: `ConnectionExecutor`

2. **Testability**
   ```rust
   // Test strategy generation without networking
   let parsed = ParsedDataSource::parse("myserver,1433", false)?;
   let chain = parsed.to_connection_actions(15000);
   assert_eq!(chain.len(), 1);
   ```

3. **Observability**
   ```rust
   // See exactly what will happen
   println!("{}", chain.describe());
   ```

4. **Extensibility**
   ```rust
   // Add new actions without changing consumer code
   enum ConnectionAction {
       // ... existing actions
       ConnectToAlwaysOnGroup { /* ... */ },  // New!
   }
   ```

5. **Mock Testing**
   ```rust
   struct MockExecutor {
       ssrp_responses: HashMap<String, u16>,
   }
   
   #[async_trait]
   impl ConnectionExecutor for MockExecutor {
       async fn query_ssrp(&self, ...) -> TdsResult<SsrpResponse> {
           Ok(SsrpResponse {
               port: *self.ssrp_responses.get(instance).unwrap(),
               ...
           })
       }
       // ... other methods return mock data
   }
   ```

## Migration Checklist

- [x] Phase 1: Add action types (COMPLETE)
  - [x] Create `ConnectionAction` enum
  - [x] Create `ConnectionActionChain` and builder
  - [x] Add `to_connection_actions()` to `ParsedDataSource`
  - [x] Define `ConnectionExecutor` trait
  - [x] Write comprehensive tests
  - [x] Create examples

- [ ] Phase 2: Implement executor
  - [ ] Create `ProductionExecutor` struct
  - [ ] Implement all `ConnectionExecutor` methods
  - [ ] Add executor tests
  - [ ] Integrate with existing transport layer

- [ ] Phase 3: Update consumers
  - [ ] Migrate `TdsConnectionProvider` to use action chains
  - [ ] Update `ClientContext` usage
  - [ ] Remove old conditional logic
  - [ ] Update integration tests

- [ ] Phase 4: Cleanup
  - [ ] Mark old methods as `#[deprecated]`
  - [ ] Update documentation
  - [ ] Add migration guide to README
  - [ ] Remove deprecated code after adoption period

## Running the Example

See the action chain design in action:

```bash
cargo run --example connection_action_chain
```

This will show connection strategies for various datasource strings.

## Questions?

The design document is available at:
- `ACTION_BASED_CONNECTION_DESIGN.md` - Full design rationale
- `examples/connection_action_chain.rs` - Working examples
- Tests in `datasource_parser.rs` - Strategy generation tests
