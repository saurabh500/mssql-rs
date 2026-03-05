# Tasks: mssql-rs Public API Crate

**Input**: Design documents from `/specs/001-mssql-rs-public-api/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/public-api.md, quickstart.md

**Tests**: Not included — not requested in the feature specification. Test files listed in plan.md (`mssql-rs/tests/`) should be created separately if needed.

**Organization**: Tasks grouped by user story (US1–US7) to enable independent implementation and testing per story.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: Which user story (US1–US7) the task belongs to

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create the `mssql-rs` crate skeleton and integrate it into the Cargo workspace.

- [ ] T001 Create mssql-rs/Cargo.toml with dependencies (mssql-tds, futures, bytes, tokio, thiserror, tracing, bigdecimal, uuid) and edition=2024 in mssql-rs/Cargo.toml
- [ ] T002 [P] Add mssql-rs to workspace members list in Cargo.toml (root)
- [ ] T003 [P] Create mssql-rs/src/lib.rs with copyright header, module declarations (client, value, row, result_set, from_value, error, metadata, datetime, column_stream), and pub use re-exports per contracts/public-api.md in mssql-rs/src/lib.rs

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core types shared by ALL user stories — Error, Value, DateTime, ColumnMetadata. Must complete before any user story.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T004 [P] Implement Error enum (9 variants) with thiserror derives, From<mssql_tds::Error> mapping, and Result<T> type alias in mssql-rs/src/error.rs
- [ ] T005 [P] Implement DateTime struct (8 optional fields: year, month, day, hour, minute, second, nanoseconds, offset_minutes) with From impls for all 6 TDS temporal types (SqlDateTime, SqlDateTime2, SqlDate, SqlTime, SqlDateTimeOffset, SqlSmallDateTime) in mssql-rs/src/datetime.rs
- [ ] T006 [P] Implement ColumnMetadata struct (name, data_type, nullable, collation) and DataType enum (11 variants) in mssql-rs/src/metadata.rs
- [ ] T007 Implement Value enum (13 variants: Null, Bool, Int, Float, Decimal, String, Binary, DateTime, Uuid, Xml, Json, Vector) with is_null() method and derive(Debug, Clone, PartialEq) in mssql-rs/src/value.rs
- [ ] T008 Implement ColumnValues → Value coalescing logic (25 wire variants → 13 Value variants per research R2 mapping table) in mssql-rs/src/value/from_column.rs

**Checkpoint**: All shared types compiled. `cargo bclippy` passes on mssql-rs.

---

## Phase 3: User Story 1 — Connect and Execute a Simple Query (Priority: P1) 🎯 MVP

**Goal**: A developer connects via an ODBC connection string and executes a query, receiving all rows as `Vec<Vec<Value>>`.

**Independent Test**: Connect to mock/live SQL Server, run `SELECT 1 AS val`, verify the returned `Vec<Vec<Value>>` contains `[[Value::Int(1)]]`.

**FRs**: FR-001 (connection string), FR-002 (ad-hoc query), FR-004 (query_collect), FR-011 (metadata), FR-012 (error type), FR-014 (cancel)

### Implementation for User Story 1

- [ ] T009 [US1] Implement ODBC connection string parser: semicolon-delimited key=value parsing, case-insensitive keys, brace-quoted values, unknown-key error, 12 standard key → ClientContext field mapping (per research R1) in mssql-rs/src/client/connection.rs
- [ ] T010 [US1] Implement Client struct with connect() (parses connection string, calls TdsConnectionProvider::create_client), pending_rollback/pending_unprepare fields, cancel(), and close() in mssql-rs/src/client.rs
- [ ] T011 [P] [US1] Implement Row struct with random-access methods: value(index), len(), is_empty(), metadata(), into_values(), internal columns Vec, and Arc<Vec<ColumnMetadata>> in mssql-rs/src/row.rs
- [ ] T012 [P] [US1] Implement ResultSet struct holding &mut Client + Arc<Vec<ColumnMetadata>>, with metadata() and collect_rows() (drains rows into Vec<Vec<Value>>) in mssql-rs/src/result_set.rs
- [ ] T013 [US1] Implement Client::query() — execute ad-hoc SQL via TdsClient, read column metadata, construct ResultSet; return empty ResultSet for DDL (FR-002) in mssql-rs/src/client/query.rs
- [ ] T014 [US1] Implement Client::query_collect() — call query() then collect_rows(), return Vec<Vec<Value>> in mssql-rs/src/client/query.rs

**Checkpoint**: `Client::connect("...").await?.query_collect("SELECT 1 AS val").await?` returns `[[Value::Int(1)]]`. US1 is end-to-end functional.

---

## Phase 4: User Story 2 — High-Performance Row Streaming (Priority: P2)

**Goal**: Stream millions of rows with constant memory overhead. Iterate columns within a row without materializing all columns. Support multiple result sets and query cancellation.

**Independent Test**: Execute a query returning 1000 rows, consume via `StreamExt::next()`, verify all rows yielded. Test `next_result_set()` with `SELECT 1; SELECT 2` batch.

**FRs**: FR-003 (two-level streaming), FR-010 (multi-result), FR-013 (timeouts), FR-014 (cancel), FR-017 (binary streaming — deferred)

### Implementation for User Story 2

- [ ] T015 [US2] Implement futures::Stream for ResultSet via manual poll_next — delegate to TdsClient::get_next_row(), convert ColumnValues to Row, handle stream exhaustion in mssql-rs/src/result_set.rs
- [ ] T016 [P] [US2] Implement Row::next_column() sequential column streaming mode — yield Value one at a time, track position, enforce mutual exclusivity with random-access methods in mssql-rs/src/row.rs
- [ ] T017 [US2] Implement ResultSet::next_result_set(self) — consume current stream, call move_to_next(), return new ResultSet or None in mssql-rs/src/result_set.rs
- [ ] T018 [P] [US2] Wire CancelHandle from mssql-tds into Client::cancel(&self) for in-flight query cancellation in mssql-rs/src/client.rs
- [ ] T019 [P] [US2] Implement Connection Timeout and Query Timeout parsing from connection string keys (seconds → Duration), set connect_timeout on ClientContext, implement query timeout via tokio::time::timeout wrapper in mssql-rs/src/client/connection.rs
- [ ] T020 [P] [US2] Create column_stream.rs with buffered binary streaming stub — define API surface for FR-017 (yields Bytes chunks from a fully-buffered column); mark as initial-buffered-only per research R4 in mssql-rs/src/column_stream.rs

**Checkpoint**: `while let Some(row) = result_set.next().await { ... }` streams rows. `next_result_set()` advances to second set. `cancel()` stops iteration.

---

## Phase 5: User Story 3 — Extensible Type System (Priority: P3)

**Goal**: Extract column values into Rust types via a conversion trait. Users can implement the trait on their own types.

**Independent Test**: `let id: i64 = row.get(0)?;` succeeds. `let name: Option<String> = row.get(1)?;` returns None for NULL. Type mismatch returns `Error::TypeConversion`.

**FRs**: FR-006 (conversion trait), FR-007 (user-implementable), FR-008 (Option\<T\>)

### Implementation for User Story 3

- [ ] T021 [US3] Define FromValue trait with `fn from_value(value: Value) -> Result<Self>` in mssql-rs/src/from_value.rs
- [ ] T022 [US3] Implement built-in FromValue for primitive types: bool, i8, i16, i32, i64, u8, u16, u32, f32, f64 — with range-check errors for narrowing conversions (e.g., i64 → i32) in mssql-rs/src/from_value.rs
- [ ] T023 [US3] Implement FromValue for complex types: String (accepts String/Xml/Json), Vec<u8>, BigDecimal, uuid::Uuid, DateTime, and Option<T: FromValue> (returns None for Null) in mssql-rs/src/from_value.rs
- [ ] T024 [US3] Wire Row::get<T: FromValue>(index) and Row::get_by_name<T: FromValue>(name) — clone Value from columns, call T::from_value(), case-insensitive name lookup via metadata in mssql-rs/src/row.rs

**Checkpoint**: `row.get::<i64>(0)?` works. `row.get::<Option<String>>(1)?` returns None for NULL. Mismatched type returns `Error::TypeConversion`.

---

## Phase 6: User Story 4 — Collect All Results as Vec\<Vec\<Value\>\> (Priority: P4)

**Goal**: Validate the convenience collection API handles edge cases — empty results, DDL, mixed types.

**Independent Test**: `query_collect("SELECT 1, 'hello', NULL")` returns `[[Int(1), String("hello"), Null]]`. DDL returns empty vec.

**FRs**: FR-004 (collect API), FR-002 (DDL returns empty ResultSet)

### Implementation for User Story 4

- [ ] T025 [US4] Ensure collect_rows() correctly handles: zero-row results (empty Vec), DDL statements (empty ResultSet, empty Vec), and columns with mixed Value variants in a single result set in mssql-rs/src/result_set.rs
- [ ] T026 [P] [US4] Add doc-test examples on Client::query_collect() and ResultSet::collect_rows() demonstrating empty, single-row, and mixed-type returns in mssql-rs/src/client/query.rs and mssql-rs/src/result_set.rs

**Checkpoint**: `query_collect` handles all edge cases from US4 acceptance scenarios.

---

## Phase 7: User Story 5 — Parameterized Queries (Priority: P5)

**Goal**: Execute queries with named parameters via sp_executesql. Prevent SQL injection.

**Independent Test**: `query_with_params("SELECT @p1 + @p2", &[("@p1", Value::Int(1)), ("@p2", Value::Int(2))])` returns `[[Value::Int(3)]]`.

**FRs**: FR-009 (parameterized queries)

### Implementation for User Story 5

- [ ] T027 [US5] Implement parameterized query execution — build sp_executesql call from SQL + named params, map Value variants to TDS parameter types, delegate to TdsClient in mssql-rs/src/client/parameterized.rs
- [ ] T028 [US5] Implement Client::query_with_params(&mut self, sql, params) — call parameterized executor, return ResultSet in mssql-rs/src/client.rs
- [ ] T029 [US5] Implement Client::query_collect_with_params(&mut self, sql, params) — call query_with_params then collect_rows in mssql-rs/src/client/query.rs

**Checkpoint**: Parameterized queries execute correctly. Injection patterns are safely transmitted as parameter values.

---

## Phase 8: User Story 6 — Prepared Statements (Priority: P6)

**Goal**: Prepare a statement once with sp_prepare, execute multiple times with sp_execute, unprepare with sp_unprepare. Deferred cleanup on Drop.

**Independent Test**: Prepare `SELECT @p1 * @p2`, execute three times with different params, verify each result, close handle.

**FRs**: FR-015 (prepare/execute/unprepare lifecycle)

### Implementation for User Story 6

- [ ] T030 [US6] Implement PreparedStatement struct (handle: i32, client: &mut Client, closed: bool) with execute(&mut self, params) → ResultSet and close(self) → Result<()> in mssql-rs/src/client/prepared.rs
- [ ] T031 [US6] Implement Client::prepare(&mut self, sql, params) — call sp_prepare, return PreparedStatement with server handle in mssql-rs/src/client.rs
- [ ] T032 [US6] Implement Drop for PreparedStatement — if not closed, push handle to Client::pending_unprepare for deferred cleanup (research R8) in mssql-rs/src/client/prepared.rs
- [ ] T033 [US6] Implement pending_unprepare drain — before any Client operation, iterate pending_unprepare handles and call sp_unprepare for each in mssql-rs/src/client.rs

**Checkpoint**: Prepare → execute × 3 → close works. Dropping without close drains unprepare on next Client operation.

---

## Phase 9: User Story 7 — Transaction Control (Priority: P7)

**Goal**: Begin/commit/rollback transactions with isolation level control. Execute queries within transaction scope. Silent rollback on Drop.

**Independent Test**: Begin → INSERT → rollback → verify row absent. Begin → INSERT → commit → verify row present.

**FRs**: FR-016 (transaction control), FR-018 (transaction-scoped queries)

### Implementation for User Story 7

- [ ] T034 [US7] Implement IsolationLevel enum (5 variants) with mapping to mssql_tds::TransactionIsolationLevel in mssql-rs/src/client/transaction.rs
- [ ] T035 [US7] Implement Transaction struct (client: &mut Client, committed: bool, rolled_back: bool) with commit(self) and rollback(self) in mssql-rs/src/client/transaction.rs
- [ ] T036 [P] [US7] Implement Client::begin_transaction() and begin_transaction_with_isolation(level) — call TdsClient begin_transaction, return Transaction in mssql-rs/src/client.rs
- [ ] T037 [US7] Implement Transaction::query(), query_with_params(), and prepare() (FR-018) — delegate to Client's inner TdsClient within the active transaction scope in mssql-rs/src/client/transaction.rs
- [ ] T038 [US7] Implement Drop for Transaction — if not committed/rolled back, set Client::pending_rollback=true + emit tracing::debug!; extend Client operation preamble to drain pending_rollback before proceeding in mssql-rs/src/client/transaction.rs and mssql-rs/src/client.rs

**Checkpoint**: Begin → query → commit works. Drop without commit triggers deferred rollback on next operation. Isolation level is set on server.

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, formatting, lint compliance, and quickstart validation.

- [ ] T039 [P] Add crate-level doc comments (//!) with overview, usage example, and feature summary to mssql-rs/src/lib.rs
- [ ] T040 [P] Add doc comments with usage examples to all public methods and types across mssql-rs/src/ (SC-005)
- [ ] T041 Run cargo bfmt and cargo bclippy against mssql-rs; fix all warnings (zero-warning discipline, Principle III)
- [ ] T042 Validate quickstart.md examples compile against the implemented API — adjust quickstart.md or implementation if any example fails

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Setup — **BLOCKS all user stories**
- **US1 (Phase 3)**: Depends on Foundational — delivers MVP
- **US2 (Phase 4)**: Depends on US1 (extends ResultSet, Row, Client)
- **US3 (Phase 5)**: Depends on US1 (needs Row to exist for get<T> wiring)
- **US4 (Phase 6)**: Depends on US1 (validates query_collect path)
- **US5 (Phase 7)**: Depends on US1 (adds parameterized variant to query path)
- **US6 (Phase 8)**: Depends on US5 (prepared statements use parameter binding)
- **US7 (Phase 9)**: Depends on US1 + US5 (transactions wrap query + parameterized paths)
- **Polish (Phase 10)**: Depends on all user stories being complete

### User Story Independence

| Story | Can Start After | Independent of |
|-------|----------------|----------------|
| US1 (P1) | Phase 2 | — |
| US2 (P2) | US1 | US3, US4, US5, US6, US7 |
| US3 (P3) | US1 | US2, US4, US5, US6, US7 |
| US4 (P4) | US1 | US2, US3, US5, US6, US7 |
| US5 (P5) | US1 | US2, US3, US4 |
| US6 (P6) | US5 | US2, US3, US4, US7 |
| US7 (P7) | US1 + US5 | US2, US3, US4, US6 |

### Parallel Opportunities After US1

Once US1 is complete, these can proceed in parallel:
- **Track A**: US2 (streaming) → then US4 depends on nothing else
- **Track B**: US3 (type system) — fully independent
- **Track C**: US5 (parameterized) → US6 (prepared) → US7 (transactions)

---

## Parallel Example: Foundational Phase

```text
# These three tasks touch different files — run in parallel:
T004 [P] Error enum in error.rs
T005 [P] DateTime struct in datetime.rs
T006 [P] ColumnMetadata in metadata.rs

# Then sequentially (Value depends on DateTime):
T007 Value enum in value.rs
T008 ColumnValues → Value coalescing in value/from_column.rs
```

## Parallel Example: After US1 Completes

```text
# Three independent tracks can start simultaneously:
Track A: T015 (Stream impl) → T016, T017, T018, T019, T020
Track B: T021 (FromValue trait) → T022, T023, T024
Track C: T027 (parameterized.rs) → T028, T029 → T030 (prepared) → ...
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001–T003)
2. Complete Phase 2: Foundational (T004–T008)
3. Complete Phase 3: US1 (T009–T014)
4. **STOP and VALIDATE**: `query_collect("SELECT 1 AS val")` works end-to-end
5. Run `cargo bclippy` — fix any warnings

### Incremental Delivery

1. Setup + Foundational → crate compiles with all type stubs
2. US1 → connect + query_collect works (MVP!)
3. US2 → streaming row iteration, cancel, timeout
4. US3 → type-safe extraction via `row.get::<T>()`
5. US4 → collect edge cases validated
6. US5 → parameterized queries
7. US6 → prepared statements
8. US7 → transactions
9. Polish → docs, lint, quickstart validation

Each story adds capability without breaking previous stories.

---

## Notes

- **42 total tasks** across 10 phases (3 setup + 5 foundational + 34 feature/polish)
- [P] tasks = different files, no dependency on incomplete tasks in the same batch
- [Story] label maps task to spec.md user story for traceability
- FR-017 (binary chunk streaming) is represented by T020 as a buffered stub; true PLP streaming requires mssql-tds changes
- FR-018 (transaction-scoped queries) is covered by T037
- Deferred cleanup (pending_rollback, pending_unprepare) is split across US6 (T032–T033) and US7 (T038) per the research decisions R7/R8
- No test tasks generated; test files in plan.md (`mssql-rs/tests/*.rs`) can be created as follow-up work
