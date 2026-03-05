<!--
  Sync Impact Report
  ==================
  Version change: 1.0.0 → 1.1.0
  Modified principles: none renamed
  Added sections:
    - Principle VI. High Performance
  Removed sections: N/A
  Templates requiring updates:
    - .specify/templates/plan-template.md — ✅ compatible
    - .specify/templates/spec-template.md — ✅ compatible
    - .specify/templates/tasks-template.md — ✅ compatible
    - .specify/templates/checklist-template.md — ✅ compatible
  Follow-up TODOs: none
-->

# mssql-tds Constitution

## Core Principles

### I. Layered Protocol Architecture

All TDS protocol functionality MUST follow the layered architecture:
Transport → IO (packet reader/writer) → Token stream → Message → Client API.

- Module organization: file `foo.rs` declares `pub mod` items;
  implementations live in `foo/` subdirectory.
- Visibility MUST be deliberate: `pub(crate)` for internal APIs,
  explicit `pub` only for the intended public surface.
- Error handling MUST use `thiserror` derive macros and the
  `TdsResult<T>` type alias.
- Async operations MUST use the Tokio runtime and `async-trait`.
- Cancellation MUST use `CancelHandle` wrapping
  `tokio_util::CancellationToken`.

Rationale: A strict layer boundary keeps the protocol implementation
testable in isolation and prevents coupling between wire-format
details and consumer-facing APIs.

### II. Rust Core, Thin FFI Bindings

The `mssql-tds` crate is the single source of protocol logic.
Language bindings (`mssql-js`, `mssql-py-core`) MUST be thin
wrappers that delegate to the core crate.

- JS bindings use `#[napi]` attributes and `Arc<Mutex<TdsClient>>`
  for thread safety.
- Python bindings use `#[pymodule]` / `#[pyclass]` via PyO3.
- `mssql-py-core` is excluded from the Cargo workspace and MUST
  receive separate `fmt` and `clippy` runs.

Rationale: One implementation of TDS guarantees behavioral
consistency across every language driver and eliminates
cross-language divergence bugs.

### III. Zero-Warning Discipline (NON-NEGOTIABLE)

Every change MUST pass the pre-push gate **before** merge:

1. `cargo bfmt` — formatting check.
2. `cargo bclippy` — lint with `-D warnings` (any warning is a
   build failure).
3. `cargo btest` — full test suite via `cargo nextest` with
   `cargo-llvm-cov`.

- CI enforces an 85 % diff-coverage target.
- The pre-commit hook auto-runs `cargo fmt` on workspace +
  `mssql-py-core` and blocks the commit on formatting drift.
- `--frozen` MUST succeed; dependency changes require an explicit
  `Cargo.lock` update.

Rationale: A zero-tolerance lint/format/test gate keeps the
codebase uniformly readable and prevents regressions from
accumulating.

### IV. Test Infrastructure

- The test runner is `cargo nextest` — never `cargo test`.
- Unit tests live in `#[cfg(test)]` inline modules for pure logic.
- Integration tests live in `tests/` directories — never in
  `#[cfg(test)]` modules.
- The `mssql-mock-tds` crate provides a mock TDS server for
  unit/integration tests without a live SQL Server.
- Integration tests requiring a live server MUST load connection
  details from a `.env` file via the `dotenv` crate.
- Kerberos tests are gated by `KERBEROS_TEST=1`.
- Python tests MUST reuse shared fixtures and env helpers from
  `conftest.py`; never invent new patterns.

Rationale: Deterministic, fast tests that can run without external
infrastructure are essential for developer velocity and CI
reliability.

### V. Code Quality — No AI Slop

- No verbose comments restating what the code does.
- No filler phrases: "This ensures that…", "In order to…",
  "It's worth noting…".
- No redundant validation or duplicate logic.
- Doc comments (`///`) MUST explain *why*, not *what*.
- Every `.rs` file MUST start with the Microsoft copyright header.
- Litmus test: would a senior Rust engineer roll their eyes?
  Then don't write it.

Rationale: Signal-to-noise ratio in code and comments directly
impacts review speed and long-term maintainability.

### VI. High Performance

This is a protocol-level driver — runtime speed is a first-class
design constraint. When a tradeoff exists between readability and
performance, favor performance, provided the code remains
understandable to a competent Rust engineer.

- Prefer zero-copy parsing and borrowing over cloning.
- Minimize heap allocations on hot paths; use stack buffers,
  `SmallVec`, or arena patterns where measurable.
- Async code MUST avoid unnecessary `Box<dyn Future>` indirection
  when static dispatch is feasible.
- Release builds MUST enable LTO and symbol stripping
  (already configured in workspace `Cargo.toml`).
- Performance-sensitive changes SHOULD include benchmark evidence
  (use the `benches/` directory in `mssql-tds`).
- Never sacrifice correctness for speed.

Rationale: A database driver sits on every query's critical path.
Microsecond-level overhead compounds across millions of calls;
performance regressions directly impact downstream applications.

## Code Conventions & Constraints

- **File header**: every `.rs` file MUST begin with
  `// Copyright (c) Microsoft Corporation.` /
  `// Licensed under the MIT License.`
- **Naming**: `Tds` prefix for core public types (`TdsClient`,
  `TdsTransport`, `TdsResult`). Standard Rust conventions:
  `snake_case` functions, `PascalCase` types,
  `SCREAMING_SNAKE_CASE` constants.
- **Tracing**: use the `tracing` crate (`debug!`, `error!`,
  `info!`, `trace!`, `#[instrument]`).
- **Authentication**: two-phase resolution (validate inputs →
  resolve method) in `connection/`. Kerberos/GSSAPI for integrated
  auth cross-platform.
- **Platform checks**: MUST grep the codebase (CI configs,
  `kerberos-test/`, `tests/`) before adding guards or rejections.
  The codebase may already handle the case cross-platform.

## Development Workflow

- **Cargo aliases** (`bfmt`, `bclippy`, `btest`) are defined in
  `.cargo/config.toml` and MUST be used instead of raw commands.
- **Scripts** `scripts/bfmt.ps1` and `scripts/bclippy.ps1` cover
  both the workspace and `mssql-py-core`.
- **Git commits**: imperative tense ("Add", "Fix", "Refactor");
  short subject lines; no conventional-commit prefixes.
- **Branches**: feature → `dev/<developer>/<feature-name>`,
  integration → `development`, default → `main`.
- **Pre-commit hook** at `dev/hooks/pre-commit`; install via
  `./setup-hooks.sh`.
- **JS workflow** (`mssql-js`): `yarn install` → `yarn build` →
  `yarn test` → `yarn lint` → `yarn format:check`.

## Governance

This constitution is the authoritative reference for project-wide
engineering standards. It supersedes ad-hoc conventions and
informal agreements.

- **Amendments** MUST be documented in this file with an updated
  version number, date, and Sync Impact Report.
- **Version numbering** follows semantic versioning:
  - MAJOR: backward-incompatible principle removal or redefinition.
  - MINOR: new principle or materially expanded guidance.
  - PATCH: clarifications, wording, or typo fixes.
- **Compliance** MUST be verified during code review; reviewers
  SHOULD reference the relevant principle when requesting changes.
- **Runtime guidance** lives in `.github/copilot-instructions.md`;
  it MUST remain consistent with the principles enumerated here.

**Version**: 1.1.0 | **Ratified**: 2026-03-04 | **Last Amended**: 2026-03-04
