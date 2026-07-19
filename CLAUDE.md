# ChartPDS

Rust workspace. A single library crate (`chartpds-core`) plus a stdio MCP
server binary (`chartpds-mcp`).

## Where things are documented

This file holds policy, workflow, and the cross-cutting invariants you need
before opening any particular file. Everything else — module design, API
surface, per-item contracts — lives in rustdoc, which is enforced (see the
`missing_docs` note under Lint policy), so module `//!` headers and item
docs are reliable and must be kept current:

- Crate map and the three storage tiers: `crates/chartpds-core/src/lib.rs`
- MCP tool surface: the `#[tool(description = "...")]` strings in
  `crates/chartpds-mcp/src/server.rs` are the canonical per-tool docs (MCP
  clients receive them verbatim). Do not enumerate tools anywhere else.
- Query primitives: catalog and recipe in
  `crates/chartpds-core/src/queries/mod.rs`; semantics in each query file
  (e.g. in-range bucket attribution in `aligned_table.rs`, episode
  detection in `episodes.rs`)
- Schema: `crates/chartpds-core/migrations/*.sql`; per-table CRUD and sqlx
  patterns in `crates/chartpds-core/src/index/mod.rs`
- Archive/manifest model: `archive/mod.rs` and `archive/manifest.rs`
- Ingestion and the narrative/LLM pipeline: `ingestion/mod.rs`,
  `ingestion/narrative.rs`, `extraction/` (CCDA parsing quirks are on the
  extractors themselves, e.g. the LDL-C `nullFlavor` fallback on
  `extract_pq_value` in `ingestion/ccda/results.rs` — do not "simplify"
  documented fallbacks away)
- Adapters (setup steps, encodings): `sources/fitbit/mod.rs`,
  `sources/oura/mod.rs`
- Sync daemon and the fetch rule: `sync/mod.rs`, `sources/confidence.rs`
- Notifications: `notifications/mod.rs`

When you change behavior, update the module/item docs in the same diff —
they are the primary documentation; this file only routes to them.

## Toolchain

- Rust stable pinned in `rust-toolchain.toml`.
- Task orchestration via `just`. Run `just check` before declaring any change
  complete — it chains `fmt-check`, `lint`, `typecheck`, `test`, `cargo deny`,
  and `cargo machete`.

## Lint policy

**Never bypass a lint rule. Fix the actual problem.**

This applies to clippy, rustc, `cargo deny`, and `cargo machete`. Specifically
forbidden:

- `#[allow(...)]` without a `reason = "..."` string. The workspace-wide
  `clippy::allow_attributes_without_reason = "deny"` enforces this.
- Per-crate lint relaxations that disable a workspace lint.
- Adding a license to `deny.toml`'s allow-list without verifying that license
  is actually acceptable for this project.

If a rule fires, the right move is to refactor — split the function, narrow
the type, use a different data shape. The rule exists for a reason; bypassing
it opts out of that reason.

Note on `missing_docs`: the workspace lint config sets it to `warn`, but
`just lint` invokes clippy with `-D warnings`, which promotes all warnings
to errors. In practice this means every `pub` item must have a doc comment
or `just check` will fail.

## Module boundaries

`chartpds-core` is the single library crate. Inside it, default visibility is
`pub(crate)` for cross-module use. Items exposed to the binary go through
`crates/chartpds-core/src/lib.rs` re-exports and are explicitly `pub`. The
binary (`chartpds-mcp`) can only call those re-exports.

Submodule internals are private by default. Cross-module access goes through
the submodule's `mod.rs`, not by reaching into private files.

## Database schema changes

The `index/` module uses sqlx with compile-time SQL verification in offline
mode: builds read the committed `.sqlx/` cache at the workspace root and
never connect to a live database. After any change to
`crates/chartpds-core/migrations/*.sql` or any
`sqlx::query!`/`sqlx::query_as!` invocation:

```sh
just prepare-sql
```

Commit the schema change and the cache update in the same commit so the
build stays hermetic. A forgotten `prepare-sql` fails `just check` (it runs
`cargo sqlx prepare --check`) rather than slipping through to runtime.
`sqlx-cli` is installed by `just install-tools`.

Migrations are forward-only — never write a down migration; ship a
correcting forward migration instead. The recurring sqlx column-type
override patterns (`"id!: i64"`, `"col?: OffsetDateTime"`) are documented
in `index/mod.rs`.

## Adding a dependency

1. Add to the crate's `Cargo.toml`, not the workspace root (unless it's
   genuinely shared by multiple crates — at which point add to
   `[workspace.dependencies]` and reference with `workspace = true`).
2. Run `just check` — `cargo deny` validates the license, `cargo machete`
   confirms it's actually used.
3. Prefer crates with permissive licenses (see `deny.toml` allow-list).

## Storage model — invariants

Three tiers (full description in the `lib.rs` crate docs): **archive**
(`$DIR/archive/`, bytes from outside; sacred, never GC'd) → **derived**
(`$DIR/derived/`, frozen machine derivations, currently LLM extraction
artifacts) → **index** (`$DIR/chartpds.db`, the disposable SQLite
projection, rebuilt from the two blob stores by `rebuild_index` — a
network-free replay, never a re-fetch or re-extraction).

Invariants that must survive any refactor:

- `archived_at` is the immutable first-entry time, carried in each blob's
  sidecar manifest and copied *through* on rebuild — never re-stamp it to
  "now".
- Write blobs with `Archive::put_with_manifest`; bare `put` is for tests
  only.
- The LLM runs exactly once, at narrative ingest, and requires
  `ANTHROPIC_API_KEY` (`ANTHROPIC_BASE_URL` optionally overrides the
  endpoint). Its mechanically verified output is frozen in the derived
  store; `rebuild_index` replays the artifact and never calls a model. If
  extraction cannot run — missing key, sustained outage — the whole ingest
  fails before anything is archived or indexed; nothing partial persists.
- LLM output is never trusted directly: every claim is verified against the
  extracted text (`extraction::verify_extraction`) and unverified claims
  are dropped, with reasons recorded in `rejected`.
- CCDA `ingest()` is deliberately non-transactional: on a mid-ingest crash,
  re-run from the archive — the bytes are durable.

## MCP server

`chartpds-mcp` reads `CHARTPDS_DATA_DIR` from the environment (a plain
absolute directory path), opens the SQLite pool at `$DIR/chartpds.db`, the
archive at `$DIR/archive/`, and the derived store at `$DIR/derived/`, and
serves the tools defined in `crates/chartpds-mcp/src/server.rs`. The
adding-a-tool recipe is in that file's header. When any adapter is
configured, the server also spawns the background sync daemon
(`sync/mod.rs`; interval via `CHARTPDS_SYNC_INTERVAL_SECS`, `0` disables).

## Queries

`queries::` holds pure analytical primitives that the binary composes into
MCP tools. Catalog, shape, and the adding-a-primitive recipe are in
`queries/mod.rs`; subtle semantics live with each query's code.

## Sources & sync

Adapter setup steps and value encodings are in each adapter's `mod.rs`
(`sources/fitbit/`, `sources/oura/`). The general fetch rule — fetch a day
unless it is **covered AND settled** — and the backfill bug that motivated
it are documented in `sources/confidence.rs`; per-adapter settledness
models live in each adapter's `confidence.rs`. Notification conditions and
re-fire cadence: `notifications/mod.rs`.

## Holdout regression suite

The `holdout/` crate is a tamper-resistant regression suite: black-box
integration tests that drive the real `chartpds-mcp` binary over stdio and
assert on the JSON the product tools return. It binds to the MCP tool surface,
NOT to `chartpds-core`'s internal API, so internal refactors do not legitimately
break it. Each test encodes a real bug we never want back. See
`docs/superpowers/specs/2026-06-27-holdout-regression-suite-design.md`.

**Three tiers of authority — treat them differently:**

1. **Spec & holdout tests — read-only.** You may NOT edit anything under
   `holdout/`, `holdout.lock`, `.github/allowed_signers`, or
   `.github/workflows/holdout.yml`. These are protected paths.
2. **Code (`crates/**`) — freely mutable.** Fix bugs here, fast.

**Operating rules:**

- **When a holdout test fails, STOP and report it. Do not touch the test.** A
  holdout failure is a real regression; fix the code in `crates/**` so the test
  passes. Never edit, `#[ignore]`, delete, or weaken a holdout test or its
  fixture to get to green.
- **Never run `just holdout-bless`.** Blessing requires a human signature
  (Touch ID); you cannot produce one, and the CI gate will reject any unsigned
  change to a protected path.
- **You may draft a holdout test only when explicitly asked.** When you do,
  write the test under `holdout/` and the fixture under `holdout/fixtures/`, run
  it to confirm it reproduces the bug, then leave the changes
  staged-but-uncommitted and hand off: tell the human it is "ready to bless."
  The human runs `just holdout-bless "<why>"` to admit it via a signed commit.
- `just check` runs the holdout tests (via `cargo test --workspace`) and the
  `holdout-verify` lockfile check. If `holdout-verify` fails during your work,
  you have modified a protected file — revert it; do not regenerate the lock.
