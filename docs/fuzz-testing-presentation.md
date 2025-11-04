---
title: Fuzz Testing Strategy for mssql-tds
author: Security and Quality Team
date: November 2025
---

# Fuzz Testing Strategy for mssql-tds

A Comprehensive Approach to Security and Quality

---

## What is Fuzz Testing?

Fuzz testing is an automated software testing technique that provides invalid, unexpected, or random data as inputs to a program.

**Goal**: Discover bugs that traditional testing misses

**Finds**:
- Crashes and panics
- Hangs and infinite loops
- Memory issues (buffer overflows, use-after-free)
- Logic errors and edge cases

---

## Why Fuzz Testing Matters

Traditional testing has limitations:

- Manual test cases cover expected behavior
- Unit tests verify known scenarios
- Integration tests check happy paths

**Fuzz testing explores the unexpected**:
- Malformed inputs
- Boundary conditions
- Unusual combinations
- Adversarial data

---

## How Fuzzing Works

```
┌─────────────────┐
│ Generate Input  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Run Program    │
│  with Input     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Monitor for     │
│ Crashes/Issues  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Mutate Input    │
│ Based on        │
│ Feedback        │
└────────┬────────┘
         │
         └──────> Repeat
```

---

## Coverage-Guided Fuzzing

Modern fuzzers use feedback to guide input generation:

**Process**:
1. Start with seed inputs
2. Execute program with instrumentation
3. Track code coverage (which paths executed)
4. Keep inputs that discover new coverage
5. Mutate interesting inputs
6. Repeat millions of times

**Result**: Systematically explores program state space

---

## Instrumentation

Instrumentation modifies code to observe execution:

**Compile-time instrumentation**:
```rust
// Original code
if x > 10 {
    do_something();
}

// Instrumented code
if x > 10 {
    __coverage_edge(123);  // Track edge
    do_something();
}
```

**Tracks**: Code blocks, edges, comparisons, memory operations

---

## Fuzzing in Rust

Rust's safety guarantees prevent many bugs, but issues remain:

**Still possible**:
- Logic errors
- Panics from unwrap/expect
- Arithmetic overflow
- Unsafe code bugs
- Denial of service

**Rust fuzzing ecosystem**:
- cargo-fuzz (primary tool)
- libFuzzer (underlying engine)
- arbitrary crate (structured input generation)

---

## libFuzzer Architecture

libFuzzer is an in-process, coverage-guided fuzzing engine:

**Components**:
- Mutation engine (bit flips, byte changes, crossover)
- Corpus management (minimization, deduplication)
- Coverage feedback (edge counters, value profiling)
- Crash detection and reporting

**Integration**: Links directly with code being tested for fast execution

---

## Structured Input Generation

The arbitrary crate enables structured fuzzing:

```rust
#[derive(Arbitrary, Debug)]
struct Packet {
    version: u8,
    packet_type: u8,
    payload: Vec<u8>,
}

fuzz_target!(|packet: Packet| {
    let bytes = packet.to_bytes();
    process_packet(&bytes);
});
```

**Benefit**: Generates valid-looking inputs that explore deeper logic

---

## Security Boundaries in mssql-tds

The mssql-tds library has multiple trust boundaries:

**External inputs (untrusted)**:
- TDS protocol packets from SQL Server
- Network data that may be malicious or malformed
- Binary data requiring careful parsing

**API inputs (semi-trusted)**:
- Application queries and parameters
- Connection strings
- Configuration options

**Both require fuzzing**

---

## TDS Protocol Security

The TDS protocol is a binary protocol with complex parsing:

**Risks**:
- Malicious SQL Server sending crafted packets
- Man-in-the-middle attacks
- Compromised network infrastructure
- Parser bugs leading to client crashes or RCE

**Why fuzz TDS parsing**:
- Protocol is complex and stateful
- Binary data with many edge cases
- Critical security boundary

---

## Current Fuzz Targets

We have implemented fuzz targets for key parsers:

**fuzz_done_token**:
- Tests DONE packet parsing
- 12-byte binary structure
- Status flags, command type, row count
- Critical for transaction boundaries

**fuzz_token_stream**:
- Tests token stream parsing
- Multiple token types
- Stateful parsing logic
- Complex control flow

---

## Fuzzing Roadmap: What APIs to Fuzz

Based on codebase analysis, we need to fuzz multiple layers:

**Layer 1: Token Parsers** (TDS Protocol)
- Network boundary - untrusted input
- Binary protocol parsing
- Critical security boundary

**Layer 2: Data Type Decoders** (Type System)
- SQL type conversion
- Complex type handling
- Memory safety critical

**Layer 3: Message Handlers** (Protocol Messages)
- Login packets
- RPC calls
- Batch commands

---

## Token Parsers to Fuzz (Priority 1)

All TDS token parsers handle untrusted network data:

**High Priority**:
- EnvChangeTokenParser (environment changes, transactions)
- ErrorTokenParser (error messages)
- InfoTokenParser (informational messages)
- ColMetadataTokenParser (column metadata)
- RowTokenParser (row data - complex)

**Medium Priority**:
- LoginAckTokenParser (login responses)
- FeatureExtAckTokenParser (feature negotiation)
- FedAuthInfoTokenParser (authentication info)
- ReturnValueTokenParser (stored procedure returns)
- OrderTokenParser (ORDER BY clauses)

---

## Token Parser Details

**EnvChangeTokenParser**:
- 18 different subtypes
- Database changes, transactions, collations
- Routing information
- Complex state transitions

**ColMetadataTokenParser**:
- Column metadata for result sets
- Type information parsing
- Collation data
- Critical for data interpretation

**RowTokenParser**:
- Actual row data parsing
- Multiple data type handling
- Large payload processing
- High complexity, high risk

---

## Data Type Decoders to Fuzz (Priority 2)

SQL type decoders handle complex type conversions:

**Critical types** (from sqldatatypes.rs):
- NVARCHAR, VARCHAR (string handling)
- VARBINARY (binary data)
- DECIMAL, NUMERIC (precision arithmetic)
- DATETIME variants (temporal parsing)
- XML (structured data)
- JSON (structured data)
- UDT (user-defined types)

**Why critical**:
- Type confusion vulnerabilities
- Buffer overflows in conversion
- Precision loss issues
- Encoding handling bugs

---

## Message Layer to Fuzz (Priority 3)

Protocol message handlers:

**Login and Authentication**:
- Login packet parsing
- Prelogin negotiation
- Feature extension handling
- Federation authentication

**Query Execution**:
- SqlBatch (SQL command batches)
- RPC parameters (stored procedures)
- Transaction management packets
- Attention packets (cancellation)

**Security boundary**: Application provides these, but malicious input possible

---

## API Surface Analysis

**Public connection APIs** (from tds_client.rs):

```rust
// Query execution
execute(sql_command, timeout, cancel_handle)
execute_sp_executesql(sql, named_params, ...)

// Row reading
read_row() -> ColumnValues
get_metadata() -> ColumnMetadata

// Transaction management
begin_transaction(isolation_level)
commit_transaction()
rollback_transaction()
```

**Fuzz strategy**: Parameter injection, malformed queries, edge cases

---

## Data Type API Surface

**Type conversion APIs** (from sqltypes.rs):

```rust
// String types
SqlType::NVarchar(data)
SqlType::Varchar(data)

// Binary types
SqlType::Varbinary(data)
SqlType::Image(data)

// Numeric types
SqlType::Decimal(precision, scale, value)
SqlType::Numeric(precision, scale, value)

// Complex types
SqlType::Xml(data)
SqlType::Udt(type_info, data)
```

**Fuzz strategy**: Boundary values, invalid encodings, overflow

---

## Work Ahead: Fuzz Target Priority Matrix

**Immediate (Weeks 1-4)**:
1. ErrorTokenParser - error handling critical
2. InfoTokenParser - similar to error tokens
3. EnvChangeTokenParser - transaction safety
4. String type decoders - common vulnerability source

**Short-term (Weeks 5-8)**:
5. ColMetadataTokenParser - result set metadata
6. Numeric type decoders - precision issues
7. LoginAckTokenParser - authentication flow
8. RPC parameter handling - injection vectors

**Medium-term (Weeks 9-16)**:
9. RowTokenParser - most complex, highest impact
10. XML/JSON parsers - structured data attacks
11. DateTime decoders - timezone and format issues
12. UDT parsers - custom type handling

---

## Fuzz Target Template

Each new fuzz target follows this pattern:

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;
use mssql_tds::fuzz_support::*;

fuzz_target!(|data: &[u8]| {
    // Set up parser context
    let context = ParserContext::default();
    
    // Create mock reader with fuzz data
    let mut reader = create_mock_reader(data);
    
    // Parse and catch panics
    let _ = TokenParser::parse(&mut reader, &context);
});
```

**Key aspects**: Isolation, cleanup, no crashes

---

## Example: ErrorToken Fuzz Target

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;

#[derive(Arbitrary, Debug)]
struct ErrorTokenInput {
    number: u32,
    state: u8,
    severity: u8,
    message_len: u16,
    message: Vec<u8>,
    server_name: Vec<u8>,
    proc_name: Vec<u8>,
    line_number: u32,
}

fuzz_target!(|input: ErrorTokenInput| {
    let bytes = serialize_error_token(input);
    let _ = parse_error_token(&bytes);
});
```

---

## Example: Type Decoder Fuzz Target

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;
use mssql_tds::datatypes::decoder::*;

fuzz_target!(|data: &[u8]| {
    // Fuzz decimal decoder
    if data.len() >= 2 {
        let precision = data[0];
        let scale = data[1];
        let value_bytes = &data[2..];
        
        let _ = decode_decimal(precision, scale, value_bytes);
    }
});
```

---

## Structured Fuzzing for Complex Types

For complex types like XML and JSON:

```rust
use arbitrary::Arbitrary;

#[derive(Arbitrary)]
struct XmlFuzzInput {
    // Valid-looking XML structure
    root_element: String,
    attributes: Vec<(String, String)>,
    nested_depth: u8,
    text_content: Vec<String>,
}

fuzz_target!(|input: XmlFuzzInput| {
    let xml = construct_xml(input);
    let _ = parse_xml_type(&xml);
});
```

**Benefit**: Generates semi-valid inputs that exercise deeper logic

---

## DoneToken Parser Example

The DoneToken parser processes completion packets:

**Structure** (12 bytes):
```
Offset | Size | Field           | Type
-------|------|-----------------|------
0      | 2    | status          | u16
2      | 2    | current_command | u16
4      | 8    | row_count       | u64
```

**Fuzz strategy**:
- Test all possible status flag combinations
- Invalid command enum values
- Boundary row counts (0, 1, MAX)

---

## What We Test

**Security properties**:
- No crashes on malformed input
- No panics from unwrap calls
- No buffer overflows
- No infinite loops

**Correctness properties**:
- Proper error handling
- Correct state transitions
- Valid output ranges
- Resource cleanup

---

## Example Fuzz Target

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() != 12 {
        return;
    }

    let status = u16::from_le_bytes([data[0], data[1]]);
    let current_command = u16::from_le_bytes([data[2], data[3]]);
    let row_count = u64::from_le_bytes([data[4..12].try_into().unwrap()]);

    // Test parsing - should not crash
    let _ = parse_done_token(status, current_command, row_count);
});
```

---

## Sanitizers

Sanitizers detect specific bug classes:

**AddressSanitizer (ASan)**:
- Buffer overflows
- Use-after-free
- Double-free
- Memory leaks

**MemorySanitizer (MSan)**:
- Uninitialized memory use

**UndefinedBehaviorSanitizer (UBSan)**:
- Integer overflow
- Null pointer dereference

---

## Running Locally

Basic fuzzing workflow:

```bash
# Install tools
rustup install nightly
cargo install cargo-fuzz

# Run fuzzer
cd mssql-tds
cargo +nightly fuzz run fuzz_done_token

# Run for specific duration
cargo +nightly fuzz run fuzz_done_token -- -max_total_time=60

# Run with multiple workers
cargo +nightly fuzz run fuzz_done_token -- -jobs=4
```

---

## CI Integration Strategy

Three-tiered approach balancing thoroughness and velocity:

**Tier 1: Pull Requests**
- Fast feedback (60 seconds per target)
- Non-blocking warnings
- Catches obvious regressions

**Tier 2: Main Branch**
- Deeper testing (30 minutes per target)
- Blocking on crashes
- Builds corpus over time

**Tier 3: Nightly**
- Extended fuzzing (8 hours per target)
- Multiple sanitizers
- Comprehensive coverage

---

## Tier 1: Pull Request Checks

**Configuration**:
- Duration: 60 seconds per target
- Workers: 1
- Trigger: Every PR
- Failure: Warning only

**Goals**:
- Fast developer feedback
- No workflow blocking
- Catch recent regressions

**Implementation**:
```yaml
- script: |
    cargo +nightly fuzz run fuzz_done_token -- \
      -max_total_time=60 -rss_limit_mb=2048
  displayName: Quick Fuzz Test
  continueOnError: true
```

---

## Tier 2: Main Branch Integration

**Configuration**:
- Duration: 30 minutes per target
- Workers: 4 parallel jobs
- Trigger: Commits to main
- Failure: Block build

**Features**:
- Corpus minimization
- Crash artifact publishing
- Coverage statistics

**Implementation**:
```yaml
- script: |
    cargo +nightly fuzz run fuzz_done_token -- \
      -max_total_time=1800 \
      -jobs=4 \
      -rss_limit_mb=8192
  displayName: Fuzz DoneToken
```

---

## Tier 3: Nightly Fuzzing

**Configuration**:
- Duration: 8 hours per target
- Workers: 8 parallel jobs
- Trigger: Daily at 2:00 AM
- Failure: Alert team

**Features**:
- Multiple sanitizers
- Coverage reports
- Corpus statistics
- Deep exploration

**Implementation**:
```yaml
schedules:
- cron: "0 2 * * *"
  displayName: Nightly Fuzz Testing
```

---

## Corpus Management

The corpus is the collection of interesting test inputs:

**Storage options**:
1. Azure Storage (persistent, shared)
2. Git LFS (versioned with code)
3. Pipeline Artifacts (per-build)

**Maintenance**:
- Minimize regularly to remove duplicates
- Seed with known edge cases
- Share across builds to improve coverage
- Archive crash-inducing inputs

---

## Corpus Workflow

```yaml
# Download previous corpus
- script: |
    az storage blob download-batch \
      --source fuzz-corpus \
      --destination mssql-tds/fuzz/corpus

# Run fuzzing (corpus grows)

# Minimize corpus
- script: |
    cargo +nightly fuzz cmin fuzz_done_token

# Upload enhanced corpus
- script: |
    az storage blob upload-batch \
      --source mssql-tds/fuzz/corpus \
      --destination fuzz-corpus
```

---

## Crash Handling

When fuzzing finds a crash:

**Detection**:
```yaml
- script: |
    if [ -d "fuzz/artifacts" ] && [ "$(ls -A fuzz/artifacts)" ]; then
      echo "Crashes found"
      exit 1
    fi
```

**Artifact collection**:
- Publish crash files
- Include stack traces
- Save reproduction instructions

**Minimization**:
```bash
cargo +nightly fuzz tmin fuzz_done_token artifacts/crash-abc123
```

---

## Crash Response Process

1. **Alert**: Team notified of crash
2. **Reproduce**: Run fuzzer with crash input
3. **Minimize**: Find smallest input that crashes
4. **Analyze**: Debug root cause
5. **Fix**: Implement patch
6. **Test**: Add regression test
7. **Verify**: Re-run fuzzer to confirm fix

---

## Performance Metrics

Track fuzzing effectiveness:

**Execution speed**:
- Target: Above 1000 exec/s
- Indicates efficient fuzzing

**Coverage growth**:
- Track unique code edges covered
- Should grow then stabilize

**Corpus size**:
- Grows with interesting inputs
- Minimize to prevent bloat

**Crash rate**:
- Crashes per 100,000 executions
- Lower is better (after fixes)

---

## Resource Requirements

**PR Checks**:
- 1 Linux build agent
- 2-3 minutes runtime
- 2 GB RAM

**Main Branch**:
- 4 core Linux agent
- 30-40 minutes runtime
- 8 GB RAM

**Nightly**:
- 8 core Linux agent
- 8-9 hours runtime
- 16 GB RAM

---

## Platform Considerations

**Linux only**:
- libFuzzer has DLL issues on Windows MSVC
- Use Linux build agents for fuzzing
- WSL2 for local Windows development

**Solutions for Windows developers**:
1. Use WSL2
2. Use Docker containers
3. Run in CI only

---

## Security Boundaries Recap

We fuzz at multiple trust boundaries:

**Network boundary**:
- TDS packets from SQL Server
- Binary protocol parsing
- Malicious or malformed data

**API boundary**:
- Application queries
- Connection parameters
- User-provided data

**Internal boundaries**:
- Token parsers
- State machines
- Data converters

---

## Implementation Strategy

**Week-by-week plan**:

**Weeks 1-2**: Infrastructure
- Set up fuzz target template
- Create mock reader utilities
- Establish CI integration (Tier 1)

**Weeks 3-6**: Token Parsers (Priority 1)
- ErrorTokenParser, InfoTokenParser
- EnvChangeTokenParser
- String decoders

**Weeks 7-10**: Metadata and Types (Priority 2)
- ColMetadataTokenParser
- Numeric decoders
- DateTime decoders

**Weeks 11-16**: Complex Parsers (Priority 3)
- RowTokenParser (most complex)
- XML/JSON parsers
- UDT handlers

---

## Coverage Metrics

Track progress with measurable goals:

**Code coverage**:
- Baseline: Current coverage of token parsers
- Target: 80% edge coverage of all parsers
- Measurement: cargo llvm-cov with fuzzing

**Fuzz targets count**:
- Current: 2 targets (DoneToken, TokenStream)
- Month 1: 6 targets (add 4 token parsers)
- Month 2: 12 targets (add type decoders)
- Month 3: 18 targets (add message handlers)

**Bug discovery rate**:
- Expect 5-10 bugs in first month
- Rate should decrease over time
- Zero production bugs from fuzzing

---

## Dictionary-Based Fuzzing

Dictionaries improve fuzzing efficiency:

```
# fuzz/fuzz.dict
token_done="\xFD"
token_doneproc="\xFE"
token_doneinproc="\xFF"
status_final="\x00\x00"
status_more="\x01\x00"
status_error="\x02\x00"
cmd_select="\xc1\x00"
cmd_insert="\xc3\x00"
```

**Usage**:
```bash
cargo +nightly fuzz run fuzz_done_token -- -dict=fuzz/fuzz.dict
```

---

## Continuous Fuzzing Options

For ongoing security:

**OSS-Fuzz** (if open source):
- Free Google Cloud infrastructure
- Automatic crash reporting
- Coverage tracking
- Regression testing

**Self-hosted**:
- Dedicated fuzzing VMs
- Corpus synchronization
- Custom dashboards
- Integration with security tools

---

## Success Metrics

**Coverage targets**:
- Month 1: 10% improvement over baseline
- Month 3: 25% improvement
- Month 6: 40% improvement

**Quality targets**:
- Zero fuzzing crashes in production
- All crashes minimized and tested
- Corpus stable for 30 days

**Performance targets**:
- Above 1000 exec/s
- Above 95% uptime for nightly runs
- Crash analysis within 24 hours

---

## Best Practices

**Seed corpus**:
- Include valid protocol messages
- Add edge cases from bug reports
- Maintain minimal, focused seeds

**Parallel fuzzing**:
- Use multiple workers
- Distribute across machines
- Share corpus between instances

**Regular maintenance**:
- Weekly: Review crashes
- Monthly: Minimize corpus, tune parameters
- Quarterly: Evaluate effectiveness, update tools

---

## Integration Roadmap

**Phase 1: PR Integration** (Week 1-2):
- Create fuzz-pr-template.yml
- Add to validation pipeline
- Configure as non-blocking
- Monitor for false positives

**Phase 2: Main Branch** (Week 3-4):
- Create fuzz-ci-template.yml
- Set up corpus storage
- Enable blocking checks
- Establish crash response process

**Phase 3: Nightly** (Week 5-6):
- Create fuzz-nightly.yml
- Configure scheduled runs
- Set up alerting
- Document review process

---

## Monitoring and Reporting

Generate reports after each run:

```yaml
- script: |
    cat > fuzz_report.md << 'EOF'
    # Fuzzing Report
    
    Date: $(date)
    Duration: 30 minutes
    Workers: 4
    
    ## Coverage
    [coverage metrics]
    
    ## Crashes Found
    [crash count and details]
    EOF
```

**Dashboard elements**:
- Coverage trends
- Crash trends
- Execution speed
- Corpus growth

---

## Troubleshooting

**Slow execution**:
- Reduce max input size
- Profile target code
- Disable expensive sanitizers initially

**Out of memory**:
- Lower RSS limit
- Reduce worker count
- Clear and restart corpus

**Low coverage growth**:
- Improve seed corpus
- Add dictionary files
- Review code structure
- Use structural fuzzing

---

## Complete Fuzzing Scope Summary

**Total fuzz targets needed**: ~25-30

**Token Parsers** (14 targets):
- Done family (3): Done, DoneInProc, DoneProc
- Metadata: ColMetadata, Order
- Messages: Error, Info, LoginAck
- Environment: EnvChange, FeatureExtAck, FedAuthInfo
- Data: Row, NbcRow, ReturnValue, ReturnStatus

**Type Decoders** (10 targets):
- Strings: NVARCHAR, VARCHAR, CHAR, NCHAR
- Binary: VARBINARY, IMAGE
- Numeric: DECIMAL, NUMERIC, MONEY, FLOAT
- Temporal: DATETIME, DATETIME2, DATETIMEOFFSET, TIME, DATE
- Complex: XML, JSON, UDT, VARIANT

**Message Handlers** (5 targets):
- Login: Prelogin, Login7, FedAuth
- Execution: SqlBatch, RPC
- Transaction: TransactionManagement
- Control: Attention

---

## Security Impact Summary

Fuzz testing strengthens security posture:

**Prevents**:
- Remote code execution via malformed packets
- Denial of service from parser hangs
- Information disclosure from memory bugs
- Client crashes from unexpected inputs

**Provides**:
- Continuous security validation
- Regression prevention
- Confidence in robustness
- Evidence of due diligence

---

## Why This Matters for mssql-tds

The library handles untrusted network data:

**Attack scenarios**:
- Malicious SQL Server instance
- Compromised network
- Man-in-the-middle attacks
- Fuzzing finds parser bugs before attackers do

**Protection layers**:
- Input validation
- Safe parsing logic
- Fuzz testing
- Security audits

---

## Conclusion

Fuzz testing is essential for mssql-tds security:

**What we achieve**:
- Automated vulnerability discovery
- Comprehensive edge case testing
- Continuous security validation
- Protection at trust boundaries

**How we achieve it**:
- Tiered CI integration
- Coverage-guided fuzzing
- Multiple sanitizers
- Persistent corpus management

**Next steps**: Implement Phase 1 PR integration

---

## Questions?

**Resources**:
- Rust Fuzz Book: https://rust-fuzz.github.io/book/
- cargo-fuzz: https://github.com/rust-fuzz/cargo-fuzz
- libFuzzer: https://llvm.org/docs/LibFuzzer.html
- Internal docs: docs/fuzz-testing-ci-strategy.md

**Contact**:
- Security team for vulnerabilities
- CI/CD team for pipeline integration
- Development team for new fuzz targets
