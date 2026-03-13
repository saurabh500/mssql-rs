# Plan: Document mssql-tds Public API for crates.io

## TL;DR

Add comprehensive doc comments to the `mssql-tds` crate's public API across **9 independent PRs** (one per category), all parallelizable. A **pre-doc PR 0** audits and tightens module visibility first to reduce the public surface. A **finalize PR 10** lands after all others merge to enable `#![warn(missing_docs)]` and verify completeness. **11 PRs total.**

Each PR workflow:
1. Create a **User Story** in ADO as a child of work item [#42206](https://sqlclientdrivers.visualstudio.com/mssql-rs/_workitems/edit/42206) using `#microsoft/azure-devops-mcp` (org: `sqlclientdrivers`, project: `mssql-rs`)
2. Create feature branch → add docs → run `cargo doc`/`cargo bclippy`/`cargo bfmt`
3. Invoke `createDraftPr` prompt → create Draft PR targeting `development`, linked to the User Story from step 1

---

## ADO Work Items

Parent: [#42206](https://sqlclientdrivers.visualstudio.com/mssql-rs/_workitems/edit/42206)

Create these **User Stories** as children of #42206 using `#microsoft/azure-devops-mcp` (org: `sqlclientdrivers`, project: `mssql-rs`). Each User Story links to its Draft PR.

| # | User Story Title | Branch |
|---|---|---|
| 0 | Audit and tighten module visibility for mssql-tds | `dev/saurabh/docs/audit-visibility` |
| 1 | Add crate metadata and infrastructure docs for mssql-tds | `dev/saurabh/docs/crate-metadata` |
| 2 | Add connection management API docs for mssql-tds | `dev/saurabh/docs/connection-api` |
| 3 | Add query execution API docs for mssql-tds | `dev/saurabh/docs/query-execution` |
| 4 | Add stored procedure API docs for mssql-tds | `dev/saurabh/docs/stored-procedures` |
| 5 | Add result set reading API docs for mssql-tds | `dev/saurabh/docs/result-sets` |
| 6 | Add bulk copy API docs for mssql-tds | `dev/saurabh/docs/bulk-copy` |
| 7 | Add transaction API docs for mssql-tds | `dev/saurabh/docs/transactions` |
| 8 | Add data types API docs for mssql-tds | `dev/saurabh/docs/data-types` |
| 9 | Add error handling, security and supporting module docs for mssql-tds | `dev/saurabh/docs/errors-security-misc` |
| 10 | Enable missing_docs lint and finalize mssql-tds docs for publishing | `dev/saurabh/docs/finalize-missing-docs` |

---

## PR Structure

PR 0 merges first (reduces public surface). PRs 1–9 are independent and parallelizable. PR 10 lands after all others merge.

---

## PR 0: Audit Module Visibility (`dev/saurabh/docs/audit-visibility`) *(merges first)*

**ADO:** Create User Story "Audit and tighten module visibility for mssql-tds" as child of #42206

**What:** Audit the five top-level `pub mod` declarations (`handler`, `ssrp`, `token`, `io`, `message`) in `lib.rs` and change those that expose only internal plumbing to `pub(crate)`. This reduces the public API surface before documentation begins, avoiding wasted effort documenting items that shouldn't be public.

**Research findings:**

| Module | Public API Usage | Action |
|---|---|---|
| `handler` | None — all items are `pub(crate)` internally | Change to `pub(crate)` |
| `ssrp` | None — public functions are stubs returning `NotImplemented` | Change to `pub(crate)` |
| `token` | Yes — `SqlCollation` returned by `TdsClient::get_collation()` and `ColumnMetadata::get_collation()` | **Keep `pub`** |
| `io` | Fuzzing only — traits are `pub(crate)`, selectively exposed via `#[cfg(fuzzing)]` | Change to `pub(crate)` (fuzzing re-exports already bypass this) |
| `message` | Mixed — `TdsVersion`, `ApplicationIntent`, `TransactionIsolationLevel`, `StreamingBulkLoadWriter` in public signatures; protocol types (`LoginRequest`, `PreloginRequest`) are internal | **Keep `pub`**, but audit submodule visibility |

**Steps:**
1. In `mssql-tds/src/lib.rs`, change `pub mod handler` → `pub(crate) mod handler`
2. Change `pub mod ssrp` → `pub(crate) mod ssrp`
3. Change `pub mod io` → `pub(crate) mod io` (verify fuzzing `#[cfg(fuzzing)]` re-exports in `lib.rs` still compile)
4. Keep `pub mod token` and `pub mod message` unchanged
5. In `message` submodules: change `pub mod login` and `pub mod prelogin` to `pub(crate) mod` if their types don't appear in public signatures
6. Run `cargo bclippy`, `cargo bfmt`, `cargo btest` — ensure no compilation errors from visibility changes
7. Run `cargo doc --no-deps -p mssql-tds` — verify reduced surface
8. Invoke `createDraftPr` prompt, link PR to User Story

**Files:**
- `mssql-tds/src/lib.rs` — module visibility changes
- `mssql-tds/src/message.rs` — submodule visibility changes (if applicable)

**Impact on PRs 1–9:** After this PR merges, fewer modules/types are public → less documentation work in PRs 3, 9.

---

## PR 1: Crate Metadata & Infrastructure (`dev/saurabh/docs/crate-metadata`)

**ADO:** Create User Story "Add crate metadata and infrastructure docs for mssql-tds" as child of #42206

**What:** crates.io publishing metadata and crate-level overview documentation.

**Steps:**
1. Add crate-level `//!` doc comment to `mssql-tds/src/lib.rs` — crate overview, feature flags (`integrated-auth`, `sspi`, `gssapi`), getting-started example (connect → query → read), module index with `[crate::module]` links
2. Update `mssql-tds/Cargo.toml` — add `repository`, `readme = "README.md"`, `keywords = ["sql-server", "tds", "mssql", "database", "async"]`, `categories = ["database", "network-programming"]`
3. Create `mssql-tds/README.md` — crates.io landing page; mirrors lib.rs overview in markdown
4. Run verification: `cargo doc --no-deps -p mssql-tds`, `cargo bclippy`, `cargo bfmt`
5. Invoke `createDraftPr` prompt, link PR to User Story

**Files:**
- `mssql-tds/src/lib.rs` — add `//!` crate doc
- `mssql-tds/Cargo.toml` — add metadata fields
- `mssql-tds/README.md` — create new

**Does NOT add `#![warn(missing_docs)]`** — that goes in PR 10 after all others merge.

---

## PR 2: Connection Management Docs (`dev/saurabh/docs/connection-api`)

**ADO:** Create User Story "Add connection management API docs for mssql-tds" as child of #42206

**What:** Document the connection establishment and configuration API.

**Steps:**
1. Add `//!` module doc to `mssql-tds/src/connection.rs`
2. Document `ClientContext` struct + all pub fields in `connection/client_context.rs` — `data_source`, `connect_timeout`, `packet_size`, `keep_alive_in_ms`, `driver_version`, `tds_authentication_method`, `encryption_options`, `database`, `user_name`, `password`, etc.
3. Document `TdsAuthenticationMethod` enum — all variants (Password, SSPI, AD*, AccessToken)
4. Document `EncryptionOptions`, `DriverVersion`, `IPAddressPreference`, `VectorVersion`, `ClientContextValidator` trait
5. Add `//!` module doc to `mssql-tds/src/connection_provider.rs`
6. Document `TdsConnectionProvider::create_client()` in `connection_provider/connection_provider.rs` — the primary entry point
7. Document `TdsClient` lifecycle methods in `connection/tds_client.rs` — `close_connection()`, `send_attention_with_timeout()`, `get_collation()`, `get_dtc_address()`
8. Document `core.rs` — `TdsResult<T>`, `CancelHandle`, `SQLServerVersion`, `Version`
9. Run verification, invoke `createDraftPr`, link PR to User Story

**Files:**
- `mssql-tds/src/connection.rs` — `//!`
- `mssql-tds/src/connection/client_context.rs` — `ClientContext`, auth enums
- `mssql-tds/src/connection_provider.rs` — `//!`
- `mssql-tds/src/connection_provider/connection_provider.rs` — `TdsConnectionProvider`
- `mssql-tds/src/connection/tds_client.rs` — lifecycle methods only (not execute/result/transaction methods)
- `mssql-tds/src/core.rs` — `TdsResult`, `CancelHandle`, `SQLServerVersion`

---

## PR 3: Query Execution Docs (`dev/saurabh/docs/query-execution`)

**ADO:** Create User Story "Add query execution API docs for mssql-tds" as child of #42206

**What:** Document plain SQL, parameterized queries, and prepared statements.

**Steps:**
1. Document `TdsClient::execute()` in `connection/tds_client.rs` — plain SQL batch execution; explain timeout_sec, cancel_handle semantics
2. Document `TdsClient::execute_sp_executesql()` — parameterized queries; how to construct `RpcParameter`
3. Document prepared statement methods — `execute_sp_prepare()` (returns handle i32), `execute_sp_execute()` (uses handle), `execute_sp_prepexec()` (prepare+execute), `execute_sp_unprepare()` (cleanup)
4. Add `//!` module doc to `mssql-tds/src/message.rs`
5. Document `RpcParameter`, `RpcProcs`, `RpcType` in `message/rpc.rs`
6. Document `SqlBatch` in `message/batch.rs` if public
7. Run verification, invoke `createDraftPr`, link PR to User Story

**Files:**
- `mssql-tds/src/connection/tds_client.rs` — execute* methods only
- `mssql-tds/src/message.rs` — `//!`
- `mssql-tds/src/message/rpc.rs` — `RpcParameter`, `RpcProcs`
- `mssql-tds/src/message/batch.rs` — `SqlBatch`

---

## PR 4: Stored Procedures Docs (`dev/saurabh/docs/stored-procedures`)

**ADO:** Create User Story "Add stored procedure API docs for mssql-tds" as child of #42206

**What:** Document stored procedure execution and output parameter retrieval.

**Steps:**
1. Document `TdsClient::execute_stored_procedure()` — positional + named params, timeout, cancel
2. Document `TdsClient::get_return_values()` and `retrieve_output_params()` — how to get output parameters after SP execution
3. Document `ReturnValue` struct in `query/result.rs` — `param_ordinal`, `param_name`, `value`, `column_metadata`, `status`
4. Run verification, invoke `createDraftPr`, link PR to User Story

**Files:**
- `mssql-tds/src/connection/tds_client.rs` — `execute_stored_procedure`, output param methods only
- `mssql-tds/src/query/result.rs` — `ReturnValue`

---

## PR 5: Result Set Reading Docs (`dev/saurabh/docs/result-sets`)

**ADO:** Create User Story "Add result set reading API docs for mssql-tds" as child of #42206

**What:** Document result set iteration, multi-result navigation, and column metadata.

**Steps:**
1. Add `//!` module doc to `mssql-tds/src/query.rs` — overview of result reading patterns
2. Document `ResultSet` trait — `next_row()`, `next_row_into()`, `get_metadata()`, `maybe_has_unread_rows()` with iteration example
3. Document `ResultSetClient` trait — `move_to_next()`, `get_current_resultset()` for multi-result-set navigation
4. Document `ColumnMetadata` struct in `query/metadata.rs`
5. Document `MultiPartName` struct in `query/metadata.rs`
6. Document `TdsClient::close_query()`
7. Run verification, invoke `createDraftPr`, link PR to User Story

**Files:**
- `mssql-tds/src/query.rs` — `//!`
- `mssql-tds/src/connection/tds_client.rs` — `ResultSet` and `ResultSetClient` trait impls, `close_query()` only
- `mssql-tds/src/query/metadata.rs` — `ColumnMetadata`, `MultiPartName`

---

## PR 6: Bulk Copy Docs (`dev/saurabh/docs/bulk-copy`)

**ADO:** Create User Story "Add bulk copy API docs for mssql-tds" as child of #42206

**What:** Document the bulk copy (bulk load) API end-to-end.

**Steps:**
1. Document `BulkCopy` struct in `connection/bulk_copy.rs` — builder lifecycle (new → configure → write_to_server), add module-level `//!`
2. Document `BulkCopyOptions` — all fields with defaults and SQL Server behavior
3. Document `BulkLoadRow` trait — what users implement; show example referencing `tests/test_bulk_copy.rs`
4. Document `ColumnMapping`, `ColumnMappingSource`
5. Document `BulkCopyProgress`, `BulkCopyResult`
6. Document `BulkCopyColumnMetadata` in `datatypes/bulk_copy_metadata.rs`
7. Document `BulkCopyError`, `BulkCopyTimeoutError`, `BulkCopyAttentionTimeoutError` in `error/bulk_copy_errors.rs`
8. Document `StreamingBulkLoadWriter` in `message/bulk_load.rs` if public (used in `BulkLoadRow` impls)
9. Run verification, invoke `createDraftPr`, link PR to User Story

**Files:**
- `mssql-tds/src/connection/bulk_copy.rs` — main bulk copy API
- `mssql-tds/src/connection/bulk_copy_state.rs` — if has public items
- `mssql-tds/src/datatypes/bulk_copy_metadata.rs` — `BulkCopyColumnMetadata`
- `mssql-tds/src/error/bulk_copy_errors.rs` — bulk copy errors
- `mssql-tds/src/message/bulk_load.rs` — `StreamingBulkLoadWriter`

---

## PR 7: Transaction Docs (`dev/saurabh/docs/transactions`)

**ADO:** Create User Story "Add transaction API docs for mssql-tds" as child of #42206

**What:** Document transaction management API.

**Steps:**
1. Document `TdsClient::begin_transaction()`, `commit_transaction()`, `rollback_transaction()`, `save_transaction()`, `has_active_transaction()`
2. Document `TransactionIsolationLevel` enum in `message/transaction_management.rs` — all variants with SQL Server semantics
3. Document `CreateTxnParams` — used in commit/rollback to atomically start a new transaction
4. Run verification, invoke `createDraftPr`, link PR to User Story

**Files:**
- `mssql-tds/src/connection/tds_client.rs` — transaction methods only
- `mssql-tds/src/message/transaction_management.rs` — `TransactionIsolationLevel`, `CreateTxnParams`

---

## PR 8: Data Types Docs (`dev/saurabh/docs/data-types`)

**ADO:** Create User Story "Add data types API docs for mssql-tds" as child of #42206

**What:** Document the SQL Server type system, value types, and type wrappers.

**Steps:**
1. Add `//!` module doc to `mssql-tds/src/datatypes.rs` — type system overview, SQL Server → Rust mapping table
2. Document `SqlType` enum in `datatypes/sqltypes.rs` — all variants with SQL Server type mapping
3. Document `ColumnValues` enum in `datatypes/column_values.rs` — the main value type users extract from rows
4. Document type wrapper structs in `datatypes/sqldatatypes.rs` — `SqlTime`, `SqlDateTime2`, `SqlDateTimeOffset`, `SqlSmallDateTime`, `SqlSmallMoney`, `SqlMoney`, `SqlDate`, `SqlJson`, `SqlString`, `SqlVector`, `SqlXml`
5. Document `RowWriter` trait and `DefaultRowWriter` in `datatypes/row_writer.rs`
6. Document `Decoder` / `Encoder` in `datatypes/decoder.rs` and `datatypes/encoder.rs` if public
7. Document `TdsValueSerializer` in `datatypes/tds_value_serializer.rs` if public
8. Run verification, invoke `createDraftPr`, link PR to User Story

**Files:**
- `mssql-tds/src/datatypes.rs` — `//!`
- `mssql-tds/src/datatypes/sqltypes.rs` — `SqlType`
- `mssql-tds/src/datatypes/column_values.rs` — `ColumnValues`
- `mssql-tds/src/datatypes/sqldatatypes.rs` — type wrappers
- `mssql-tds/src/datatypes/row_writer.rs` — `RowWriter`
- `mssql-tds/src/datatypes/decoder.rs`, `encoder.rs` — if public
- `mssql-tds/src/datatypes/tds_value_serializer.rs` — if public
- `mssql-tds/src/datatypes/sql_json.rs`, `sql_string.rs`, `sql_vector.rs` — specialized types

---

## PR 9: Error Handling, Security & Supporting Modules Docs (`dev/saurabh/docs/errors-security-misc`)

**ADO:** Create User Story "Add error handling, security and supporting module docs for mssql-tds" as child of #42206

**What:** Document error types, security/auth API, and remaining supporting modules.

**Steps:**
1. Document `Error` enum in `error/mod.rs` — fill gaps on all variants; add module-level `//!` overview
2. Document `SqlErrorInfo` struct
3. Verify/complete `security/mod.rs` docs — `SecurityContext` trait, `create_security_context()`, `IntegratedAuthConfig`, platform-specific types
4. Add `//!` to `handler.rs`, document `handler_factory.rs` / `sspi_handler.rs` if public
5. Verify `io.rs` docs (already excellent — confirm accuracy)
6. Verify `sql_identifier.rs` docs (already good — confirm)
7. Add `//!` to `token.rs` — `Tokens` enum, `SqlCollation` if user-facing
8. Add `//!` to `ssrp.rs` — SQL Server Resolution Protocol
9. Document `message/login_options.rs` — `LoginOptions`, `ApplicationIntent`, `TdsVersion` if public
10. Run verification, invoke `createDraftPr`, link PR to User Story

**Files:**
- `mssql-tds/src/error/mod.rs` — `Error`, `SqlErrorInfo`
- `mssql-tds/src/security/mod.rs` — verify/complete
- `mssql-tds/src/handler.rs` — `//!`
- `mssql-tds/src/handler/handler_factory.rs` — if public
- `mssql-tds/src/io.rs` — verify only
- `mssql-tds/src/sql_identifier.rs` — verify only
- `mssql-tds/src/token.rs` — `//!`
- `mssql-tds/src/ssrp.rs` — `//!`
- `mssql-tds/src/message/login_options.rs` — login types

---

## PR 10: Finalize — Missing Docs Lint & Verification (`dev/saurabh/docs/finalize-missing-docs`) *(depends on PRs 1–9)*

**ADO:** Create User Story "Enable missing_docs lint and finalize mssql-tds docs for publishing" as child of #42206

**What:** Enable `#![warn(missing_docs)]`, fix any remaining gaps, final publish-readiness check.

**Steps:**
1. Add `#![warn(missing_docs)]` to `mssql-tds/src/lib.rs`
2. Run `cargo bclippy` — fix ALL missing_docs warnings
3. Run `cargo doc --no-deps -p mssql-tds` — verify rendering, no broken links
4. Run `cargo package --dry-run -p mssql-tds` — verify publishable
5. Manual review of HTML output in `target/doc/mssql_tds/`
6. Invoke `createDraftPr`, link PR to User Story

**Files:**
- `mssql-tds/src/lib.rs` — add `#![warn(missing_docs)]`
- Any files with remaining undocumented public items

---

## Conflict Avoidance

PRs 3, 4, 5, and 7 all touch `connection/tds_client.rs` but document **different methods**. To avoid conflicts:
- Each PR should only add `///` doc comments directly above methods it owns (listed in that PR's scope)
- No PR should reformat or reorganize the file
- If unavoidable minor conflicts occur, they will be trivial doc-comment-only merges

---

## Verification (per PR)

1. `cargo doc --no-deps -p mssql-tds` — renders correctly, no broken links
2. `cargo bclippy` — zero warnings
3. `cargo bfmt` — clean formatting

---

## Decisions

- **Doc style:** Terse, no-slop convention. Explain **what** (behavior, parameters, return values, defaults) and **why** (when to use, trade-offs, relationship to other APIs). No filler phrases — every sentence carries information. These are public API docs for external crates.io consumers who lack codebase context.
- **Examples:** Use `# Examples` sections with `no_run` or `ignore` code blocks (require live SQL Server). Mirror patterns from existing integration tests.
- **Internal modules:** Only document public items. Items behind `pub(crate)` don't need `///` docs.
- **`#![warn(missing_docs)]`:** Deferred to PR 10 after all other PRs merge, to avoid build failures in parallel PRs.
- **Scope:** Documentation only — no API changes, no refactoring, no new features.
- **Branch naming:** `dev/saurabh/docs/<category-slug>` off `development`
- **PR creation:** Use `createDraftPr` prompt (`.github/prompts/createDraftPr.prompt.md`) targeting `development` branch
- **ADO:** Each PR gets a User Story child of #42206, created via `#microsoft/azure-devops-mcp`. Draft PR linked to its User Story.

## Further Considerations

1. **Add a `prelude` module?** Re-exporting core types improves ergonomics. Could go in PR 1 or a separate PR.
2. **Feature flag doc badges.** Use `#[cfg_attr(docsrs, doc(cfg(feature = "...")))]` on items gated behind `integrated-auth`/`sspi`/`gssapi`. Could go in PR 2 (connection) or PR 9 (security).
