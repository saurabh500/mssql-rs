# Tasks: mssql-rs Integration Tests

**Input**: Design documents from `/specs/002-mssql-rs-integration-tests/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, quickstart.md

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)
- Include exact file paths in descriptions

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Add dev-dependencies and create the `tests/` directory structure

- [X] T001 Add `dotenv`, `tracing-subscriber`, and `futures` to dev-dependencies in `mssql-rs/Cargo.toml`
- [X] T002 Create `mssql-rs/tests/` directory structure with empty placeholder files for all 7 test modules

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared test helper module — all user stories depend on this

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T003 Implement `connection_string()`, `connect()`, `require_connect()`, and `init_tracing()` helpers in `mssql-rs/tests/common/mod.rs`

**Checkpoint**: Helper module ready — user story test files can now be implemented in parallel

---

## Phase 3: User Story 1 — Connection and Basic Query Tests (Priority: P1) 🎯 MVP

**Goal**: Validate connection string parsing (pure-logic) and basic connect/query/close (server-dependent)

**Independent Test**: `cargo nextest run -p mssql-rs --test connection`

- [X] T004 [P] [US1] Implement `test_connect_and_select_one` — valid connection string, `query_collect("SELECT 1 AS val")` returns `Value::Int(1)` in `mssql-rs/tests/connection.rs`
- [X] T005 [P] [US1] Implement `test_connect_missing_server_key` — missing `Server` key returns `Error::ConnectionStringInvalid` in `mssql-rs/tests/connection.rs`
- [X] T006 [P] [US1] Implement `test_connect_unknown_key` — unknown key (e.g., `Bogus=true`) returns `Error::ConnectionStringInvalid` in `mssql-rs/tests/connection.rs`
- [X] T007 [P] [US1] Implement `test_connect_brace_quoted_password` — password `{my;pass=word}` parses correctly, connection succeeds in `mssql-rs/tests/connection.rs`
- [X] T008 [P] [US1] Implement `test_connect_unreachable_server_timeout` — unreachable server returns error within bounded time in `mssql-rs/tests/connection.rs`
- [X] T009 [P] [US1] Implement `test_close_connection` — `Client::close()` completes without error in `mssql-rs/tests/connection.rs`
- [X] T010 [P] [US1] Implement `test_ddl_returns_empty_vec` — `CREATE TABLE #temp (id INT)` returns empty `Vec` in `mssql-rs/tests/connection.rs`
- [X] T011 [P] [US1] Implement `test_connection_timeout_respected` — `Connection Timeout` value is honored (FR-014) in `mssql-rs/tests/connection.rs`
- [X] T012 [P] [US1] Implement `test_query_timeout_respected` — `Command Timeout` value is honored (FR-014) in `mssql-rs/tests/connection.rs`

**Checkpoint**: Connection tests pass — `cargo nextest run -p mssql-rs --test connection`

---

## Phase 4: User Story 2 — Row Streaming and Multiple Result Sets (Priority: P2)

**Goal**: Validate `ResultSet` streaming via `futures::Stream` and `next_result_set()` navigation

**Independent Test**: `cargo nextest run -p mssql-rs --test streaming`

- [X] T013 [P] [US2] Implement `test_stream_100_rows` — iterate 100 rows via `StreamExt::next()`, count matches in `mssql-rs/tests/streaming.rs`
- [X] T014 [P] [US2] Implement `test_multi_result_set_two_sets` — `SELECT 1; SELECT 2` batch, consume first set, `next_result_set()` returns second in `mssql-rs/tests/streaming.rs`
- [X] T015 [P] [US2] Implement `test_multi_result_set_three_sets_then_none` — 3 result sets, `next_result_set()` after third returns `None` in `mssql-rs/tests/streaming.rs`
- [X] T016 [P] [US2] Implement `test_drop_result_set_mid_iteration` — drop `ResultSet` with rows remaining, client still usable in `mssql-rs/tests/streaming.rs`
- [X] T017 [P] [US2] Implement `test_stream_zero_rows` — empty result set, `StreamExt::next()` returns `None` immediately in `mssql-rs/tests/streaming.rs`
- [X] T018 [P] [US2] Implement `test_collect_rows` — `collect_rows()` drains into `Vec<Vec<Value>>` in `mssql-rs/tests/streaming.rs`

**Checkpoint**: Streaming tests pass — `cargo nextest run -p mssql-rs --test streaming`

---

## Phase 5: User Story 3 — Type System and Value Conversion (Priority: P3)

**Goal**: Validate SQL type → `Value` coalescing and `FromValue` trait for all 16 impls

**Independent Test**: `cargo nextest run -p mssql-rs --test type_system`

- [X] T019 [P] [US3] Implement `test_value_int` — `INT` column returns `Value::Int`, `row.get::<i64>()` returns correct value in `mssql-rs/tests/type_system.rs`
- [X] T020 [P] [US3] Implement `test_value_float` — `FLOAT` column, `row.get::<f64>()` returns approximately correct value in `mssql-rs/tests/type_system.rs`
- [X] T021 [P] [US3] Implement `test_value_string` — `NVARCHAR` column, `row.get::<String>()` returns correct string in `mssql-rs/tests/type_system.rs`
- [X] T022 [P] [US3] Implement `test_value_binary` — `VARBINARY` column, `row.get::<Vec<u8>>()` returns correct bytes in `mssql-rs/tests/type_system.rs`
- [X] T023 [P] [US3] Implement `test_value_uuid` — `UNIQUEIDENTIFIER` column, `row.get::<uuid::Uuid>()` matches in `mssql-rs/tests/type_system.rs`
- [X] T024 [P] [US3] Implement `test_value_decimal` — `DECIMAL` column, `row.get::<BigDecimal>()` preserves precision/scale in `mssql-rs/tests/type_system.rs`
- [X] T025 [P] [US3] Implement `test_value_datetime` — `DATETIME2` column, `row.get::<DateTime>()` has correct fields in `mssql-rs/tests/type_system.rs`
- [X] T026 [P] [US3] Implement `test_value_bool` — `BIT` column with value `1`, `row.get::<bool>()` returns `true` in `mssql-rs/tests/type_system.rs`
- [X] T027 [P] [US3] Implement `test_null_option` — `NULL` column, `row.get::<Option<String>>()` returns `None` in `mssql-rs/tests/type_system.rs`
- [X] T028 [P] [US3] Implement `test_null_non_optional_error` — `NULL` column, `row.get::<String>()` returns `Error::TypeConversion` in `mssql-rs/tests/type_system.rs`
- [X] T029 [P] [US3] Implement `test_type_mismatch_error` — `VARCHAR` column, `row.get::<i64>()` returns `Error::TypeConversion` in `mssql-rs/tests/type_system.rs`
- [X] T030 [P] [US3] Implement `test_narrowing_overflow_error` — `BIGINT` `i64::MAX`, `row.get::<i32>()` returns `Error::TypeConversion` in `mssql-rs/tests/type_system.rs`
- [X] T031 [P] [US3] Implement `test_mixed_type_row` — single row with mixed columns, `row.value(index)` returns correct `Value` variant per column in `mssql-rs/tests/type_system.rs`
- [X] T032 [P] [US3] Implement `test_from_value_all_int_widths` — test `FromValue` for `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32` (FR-007) in `mssql-rs/tests/type_system.rs`
- [X] T033 [P] [US3] Implement `test_from_value_floats` — test `FromValue` for `f32`, `f64` (FR-007) in `mssql-rs/tests/type_system.rs`

**Checkpoint**: Type system tests pass — `cargo nextest run -p mssql-rs --test type_system`

---

## Phase 6: User Story 4 — Column Metadata and Row Access Patterns (Priority: P4)

**Goal**: Validate column metadata inspection and random-access vs sequential row access

**Independent Test**: `cargo nextest run -p mssql-rs --test metadata`

- [X] T034 [P] [US4] Implement `test_metadata_column_names_and_types` — `SELECT 1 AS id, 'alice' AS name`, inspect metadata for 2 entries with correct names and `DataType` variants in `mssql-rs/tests/metadata.rs`
- [X] T035 [P] [US4] Implement `test_get_by_name_case_insensitive` — `row.get_by_name::<i64>("id")` and `row.get_by_name::<i64>("ID")` both return `1i64` in `mssql-rs/tests/metadata.rs`
- [X] T036 [P] [US4] Implement `test_get_by_name_nonexistent` — `row.get_by_name::<i64>("nonexistent")` returns error in `mssql-rs/tests/metadata.rs`
- [X] T037 [P] [US4] Implement `test_get_out_of_bounds` — `row.get::<i64>(5)` on 3-column row returns error in `mssql-rs/tests/metadata.rs`
- [X] T038 [P] [US4] Implement `test_next_column_sequential` — 3-column row, `next_column()` four times: 3 `Some`, 1 `None` in `mssql-rs/tests/metadata.rs`
- [X] T039 [P] [US4] Implement `test_into_values` — `row.into_values()` returns `Vec<Value>` with all columns in `mssql-rs/tests/metadata.rs`
- [X] T040 [P] [US4] Implement `test_len_and_is_empty` — `row.len()` and `row.is_empty()` match column count in `mssql-rs/tests/metadata.rs`

**Checkpoint**: Metadata tests pass — `cargo nextest run -p mssql-rs --test metadata`

---

## Phase 7: User Story 5 — Parameterized Queries (Priority: P5)

**Goal**: Validate parameterized queries via sp_executesql with injection safety

**Independent Test**: `cargo nextest run -p mssql-rs --test parameterized`

- [X] T041 [P] [US5] Implement `test_params_arithmetic` — `SELECT @p1 + @p2` with `10, 20` returns `Value::Int(30)` in `mssql-rs/tests/parameterized.rs`
- [X] T042 [P] [US5] Implement `test_params_injection_safety` — string param `'; DROP TABLE --` returned verbatim (FR-009) in `mssql-rs/tests/parameterized.rs`
- [X] T043 [P] [US5] Implement `test_params_null` — `Value::Null` parameter transmitted correctly in `mssql-rs/tests/parameterized.rs`
- [X] T044 [P] [US5] Implement `test_params_multi_type` — Int, String, Float, Bool, Decimal, Uuid, DateTime, Binary params all round-trip correctly in `mssql-rs/tests/parameterized.rs`
- [X] T045 [P] [US5] Implement `test_params_count_mismatch` — 2 expected, 1 provided, returns `Error::QueryFailed` in `mssql-rs/tests/parameterized.rs`
- [X] T046 [P] [US5] Implement `test_params_streaming` — `query_with_params` streaming variant, iterate via `StreamExt::next()` in `mssql-rs/tests/parameterized.rs`

**Checkpoint**: Parameterized query tests pass — `cargo nextest run -p mssql-rs --test parameterized`

---

## Phase 8: User Story 6 — Prepared Statement Lifecycle (Priority: P6)

**Goal**: Validate prepare/execute/close lifecycle and deferred unprepare on Drop

**Independent Test**: `cargo nextest run -p mssql-rs --test prepared`

- [X] T047 [P] [US6] Implement `test_prepare_execute` — prepare `SELECT @p1 * @p2`, execute with `[3, 7]`, result is `Value::Int(21)` in `mssql-rs/tests/prepared.rs`
- [X] T048 [P] [US6] Implement `test_prepare_execute_multiple_times` — execute same prepared statement 3 times with different params in `mssql-rs/tests/prepared.rs`
- [X] T049 [P] [US6] Implement `test_prepare_close` — `close()` completes without error in `mssql-rs/tests/prepared.rs`
- [X] T050 [P] [US6] Implement `test_prepare_drop_deferred_unprepare` — drop without `close()`, subsequent query succeeds (FR-010a) in `mssql-rs/tests/prepared.rs`
- [X] T051 [P] [US6] Implement `test_prepare_execute_streaming` — execute with streaming, rows yielded via `StreamExt::next()` in `mssql-rs/tests/prepared.rs`

**Checkpoint**: Prepared statement tests pass — `cargo nextest run -p mssql-rs --test prepared`

---

## Phase 9: User Story 7 — Transaction Control (Priority: P7)

**Goal**: Validate begin/commit/rollback, isolation levels, and deferred rollback on Drop

**Independent Test**: `cargo nextest run -p mssql-rs --test transactions`

- [X] T052 [P] [US7] Implement `test_transaction_commit_persists` — insert into `#temp`, commit, row visible in `mssql-rs/tests/transactions.rs`
- [X] T053 [P] [US7] Implement `test_transaction_rollback_reverts` — insert into `#temp`, rollback, row absent in `mssql-rs/tests/transactions.rs`
- [X] T054 [P] [US7] Implement `test_transaction_read_uncommitted` — begin with `IsolationLevel::ReadUncommitted`, query succeeds in `mssql-rs/tests/transactions.rs`
- [X] T055 [P] [US7] Implement `test_transaction_snapshot` — begin with `IsolationLevel::Snapshot`, skip on SQL error 3952 (R5) in `mssql-rs/tests/transactions.rs`
- [X] T056 [P] [US7] Implement `test_transaction_drop_deferred_rollback` — drop without commit/rollback, subsequent query succeeds (FR-010b) in `mssql-rs/tests/transactions.rs`
- [X] T057 [P] [US7] Implement `test_transaction_parameterized_query` — `txn.query_with_params()` within transaction scope in `mssql-rs/tests/transactions.rs`
- [X] T058 [P] [US7] Implement `test_transaction_prepared_statement` — `txn.prepare()` within transaction scope in `mssql-rs/tests/transactions.rs`

**Checkpoint**: Transaction tests pass — `cargo nextest run -p mssql-rs --test transactions`

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: Cancel test, edge cases, and final validation

- [X] T059 [P] Implement `test_cancel_inflight_query` — `Client::cancel()` interrupts a long-running query (FR-013) in `mssql-rs/tests/connection.rs`
- [X] T060 [P] Implement `test_unicode_supplementary_roundtrip` — emoji/CJK strings round-trip through params and results in `mssql-rs/tests/parameterized.rs`
- [X] T061 Run `cargo bfmt` and `cargo bclippy` — fix any warnings (FR-015)
- [X] T062 Run full suite via `cargo btest -p mssql-rs` — all tests pass
- [X] T063 Validate quickstart.md instructions by running the documented commands

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 — BLOCKS all user stories
- **User Stories (Phases 3–9)**: All depend on Phase 2 completion
  - User stories can proceed in parallel (all write to different files)
  - Or sequentially in priority order (P1 → P2 → … → P7)
- **Polish (Phase 10)**: Depends on all user story phases being complete

### User Story Dependencies

- **US1 (Phase 3)**: Independent — no dependency on other stories
- **US2 (Phase 4)**: Independent — no dependency on other stories
- **US3 (Phase 5)**: Independent — no dependency on other stories
- **US4 (Phase 6)**: Independent — no dependency on other stories
- **US5 (Phase 7)**: Independent — no dependency on other stories
- **US6 (Phase 8)**: Independent — no dependency on other stories
- **US7 (Phase 9)**: Independent — no dependency on other stories

### Within Each User Story

All tasks within a story are marked [P] because they are independent test functions in the same file with no inter-test dependencies.

### Parallel Opportunities

- All 7 user story phases (Phases 3–9) can execute in parallel after Phase 2
- All tasks within any phase marked [P] can execute in parallel
- Maximum parallelism: 7 concurrent streams (one per test file)

---

## Parallel Example: User Story 3 (Type System)

```text
# All of these can run simultaneously — independent test functions in one file:
T019: test_value_int
T020: test_value_float
T021: test_value_string
T022: test_value_binary
T023: test_value_uuid
T024: test_value_decimal
T025: test_value_datetime
T026: test_value_bool
T027: test_null_option
T028: test_null_non_optional_error
T029: test_type_mismatch_error
T030: test_narrowing_overflow_error
T031: test_mixed_type_row
T032: test_from_value_all_int_widths
T033: test_from_value_floats
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001–T002)
2. Complete Phase 2: Foundational (T003)
3. Complete Phase 3: User Story 1 (T004–T012)
4. **STOP and VALIDATE**: `cargo nextest run -p mssql-rs --test connection`
5. Pure-logic tests pass without server; server tests pass with `MSSQL_RS_TEST_CONNECTION_STRING`

### Incremental Delivery

1. Setup + Foundational → Helper module ready
2. US1 (connection) → Foundation verified
3. US2 (streaming) → Core I/O path verified
4. US3 (type system) → Data contract verified
5. US4 (metadata) → Schema access verified
6. US5 (parameterized) → Injection safety verified
7. US6 (prepared) → Lifecycle + deferred cleanup verified
8. US7 (transactions) → Data integrity verified
9. Polish → Cancel, unicode, lint, full suite

### Parallel Team Strategy

With 7 developers after Phase 2:

- Developer A: US1 (`connection.rs`)
- Developer B: US2 (`streaming.rs`)
- Developer C: US3 (`type_system.rs`)
- Developer D: US4 (`metadata.rs`)
- Developer E: US5 (`parameterized.rs`)
- Developer F: US6 (`prepared.rs`)
- Developer G: US7 (`transactions.rs`)

All stories write to different files — zero merge conflicts.
