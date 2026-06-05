# ChartPDS

Rust workspace. A single library crate (`chartpds-core`) plus a stdio MCP
server binary (`chartpds-mcp`).

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

The `index/` module uses sqlx with compile-time SQL verification in
**offline mode**. `cargo build` reads from a committed `.sqlx/query-*.json`
cache; it does NOT connect to a live database. The cache lives in `.sqlx/`
at the workspace root.

The index currently has 9 tables: `source_documents`, `observations`,
`problems`, `medications` (populated by ingestion), `source_credentials`,
`source_state`, `source_day_state` (populated by the adapter/sync layer),
plus `notification_state` and `notification_log` (populated by the
notification system).

After any change to `crates/chartpds-core/migrations/*.sql` or any
`sqlx::query!`/`sqlx::query_as!` invocation:

```sh
just prepare-sql
```

This rebuilds a temporary SQLite from migrations and asks `sqlx-cli` to
capture every query's schema (including test-only queries) into `.sqlx/`.
Commit both the schema change and the cache update in the same commit so
the build stays hermetic.

`sqlx-cli` is installed by `just install-tools`. The `_verify-tools` gate
on `just check` will tell you if it's missing. `just check` also runs
`cargo sqlx prepare --check`, so a forgotten `prepare-sql` after a
schema or query change will fail the lint gate rather than slip through
to runtime.

### sqlx column-type overrides

sqlx's compile-time inference reads SQLite metadata, which is sparse on
nullability. Two patterns recur:

- **`INTEGER PRIMARY KEY` and `REFERENCES` columns** are inferred as
  `Option<i64>` because SQLite doesn't carry `NOT NULL` on rowid aliases.
  Force the non-null type with `RETURNING id AS "id!: i64"` (or
  `SELECT id AS "id!: i64"`).
- **Nullable timestamp columns** mapped to `OffsetDateTime` need the
  nullable suffix: `effective_end AS "effective_end?: OffsetDateTime"`.
  The `?:` syntax tells sqlx the column is `Option<T>`.

### Migration policy

Migrations are forward-only. There are no down migrations and no
`*.down.sql` files. If a migration turns out to be wrong, ship a
follow-up forward migration that corrects it. The archive-as-truth model
makes rebuilds cheap, so schema mistakes are recoverable by re-ingest.

## Adding a dependency

1. Add to the crate's `Cargo.toml`, not the workspace root (unless it's
   genuinely shared by multiple crates — at which point add to
   `[workspace.dependencies]` and reference with `workspace = true`).
2. Run `just check` — `cargo deny` validates the license, `cargo machete`
   confirms it's actually used.
3. Prefer crates with permissive licenses (see `deny.toml` allow-list).

## Ingestion

`ingestion::ingest()` is the canonical write path: archive blob ->
parse CCDA -> extract observations + problems + medications -> write
`source_documents` row and one row per extracted item. It does NOT use
a transaction; if the process crashes mid-ingest, re-run from the
archive (the bytes are durable). Transactional ingestion lands when
`sources/` or `sync/` need atomic multi-table writes.

Four CCDA sections are extracted today: vital signs and lab results
(both stored as observations), problems (diagnoses), and medications
(prescriptions). Other sections (allergies, procedures) get their own
follow-up phases when needed.

Vital signs (LOINC 8716-3) and lab results (LOINC 30954-2) both land in
the `observations` table — a lab draw (HbA1c, LDL-C, glucose, …) is just
an observation with a lab LOINC code. The results extractor
(`ccda/results.rs`) handles two real-world wrinkles the vitals path never
hit: lab `effectiveTime` values often omit the timezone offset (the
14-char `YYYYMMDDHHmmss` form, treated as UTC by `ccda/time.rs`), and lab
`<value>` elements use a wider set of `xsi:type`s (`PQ`, `ST`, `CD`,
`IVL_PQ`). It is deliberately permissive: an unrecognized value type or a
`nullFlavor` quantity records a valueless draw (code + time) rather than
failing the whole document. Note Epic encodes calculated LDL-C as a
`nullFlavor` `PQ` whose number lives in a nested `<translation value=…>`;
`extract_pq_value` recovers it, so do not "simplify" that fallback away.

## Queries

`queries::` holds generic analytical primitives over the index.
Currently: `latest_by_code`, `in_range`, `counts_per_code`,
`list_problems`, `list_medications`. Each is a pure async function
`(&SqlitePool, args) -> Result<T, sqlx::Error>`;
there is no shared state and no struct-style query builder.

These are composed into named MCP tools (and later CLI subcommands)
by the binary crate. Adding a new query: drop a new file under
`crates/chartpds-core/src/queries/`, add a `mod` declaration and a
`pub use` re-export in `queries/mod.rs`, run `just prepare-sql` to
cache the new SQL.

## MCP server

`chartpds-mcp` is the stdio MCP server binary. It reads
`CHARTPDS_DATA_DIR` from the environment (a plain absolute directory path,
e.g. `/path/to/chartpds-data`), opens the SQLite pool at `$DIR/chartpds.db`
and the local-FS archive at `$DIR/archive/`, and serves 10 tools:

- `ingest_record` — ingest a CCDA document (write path)
- `latest_observation_by_code` — most-recent observation by LOINC code
- `observations_in_range` — observations in a time window
- `observation_counts` — count observations per code
- `list_problems` — all problems (diagnoses)
- `list_medications` — all medications
- `connect_source` — connect a data source (fitbit OAuth or oura PAT)
- `sync_source` — sync one or all configured data sources
- `rebuild_index` — drop and rebuild the index from archived blobs
- `list_notifications` — list recent notification log entries (newest first)

Adding a new tool: define an `async fn` on the `ChartPdsServer` impl
inside the `#[tool_router]` block in `crates/chartpds-mcp/src/server.rs`.
Annotate with `#[tool(description = "...")]`. Take args via
`Parameters<YourArgs>` where `YourArgs: Deserialize + JsonSchema`.
Return `Result<CallToolResult, McpError>`. Test by constructing the
server directly in `#[tokio::test]` and calling the method — no need
to go through the stdio transport.

## Fitbit adapter

Heart-rate data from Fitbit flows through Google's Health API. Setup:

1. Create a Google Cloud project with the Health API enabled.
2. Create an OAuth 2.0 "Desktop app" client. Set `GOOGLE_HEALTH_CLIENT_ID`
   and `GOOGLE_HEALTH_CLIENT_SECRET` in your environment.
3. Call `connect_source` with `source="fitbit"`. Open the URL in a browser
   and authorize. The server automatically catches the callback and stores
   credentials.
4. Call `sync_source` with `source="fitbit"` (optionally with `window_days`)
   to pull recent data.

The adapter code lives in `crates/chartpds-core/src/sources/fitbit/`.

## Oura adapter

Sleep-stage data from the Oura ring via the Oura v2 API. Setup:

1. Generate a personal access token (PAT) at
   <https://cloud.ouraring.com/personal-access-tokens>.
2. Either set `OURA_PERSONAL_ACCESS_TOKEN` in your environment or call
   `connect_source` with `source="oura"` and `token="<your PAT>"`.
3. Call `sync_source` with `source="oura"` (optionally with `window_days`)
   to pull recent sleep sessions.

Oura's `sleep_phase_5_min` string encodes per-epoch (5-minute)
sleep stages as single characters: `1` = deep (AASM N3), `2` = light
(N2), `3` = REM, `4` = awake. Each epoch becomes an observation with
`coding_system` = `https://chartpds.fhwang.net/coding/aasm/sleep-stage`,
`coding_code` = `aasm-sleep-stage`, `value_string` = stage name (e.g.
`"n3"`), and `value_quantity` = stage discriminant (e.g. `3.0`).

The adapter code lives in `crates/chartpds-core/src/sources/oura/`.

## Source trait

The `sources::Source` trait defines the shared interface for external
data adapters: `name()`, `display_name()`, and `sync(archive, pool,
window_days)`. Each source owns its auth internally (OAuth for Fitbit,
PAT for Oura). The trait uses native `impl Future` return types (not
`async_trait`) since we only have a small fixed set of sources and do
not need runtime polymorphism (`dyn Source`). The daemon tick iterates
over concrete source types via `sync_one::<S: Source>()`.

## Sync daemon

When any source adapter is configured (Google Health credentials for
Fitbit, or an Oura PAT), the MCP server automatically spawns a
background sync daemon that calls each adapter's `sync()` method on a
configurable interval (default 5 minutes). The daemon is a
`tokio::spawn`ed task that runs alongside the MCP server; it dies
when the process exits.

Set `CHARTPDS_SYNC_INTERVAL_SECS` to change the interval. Set to
`0` to disable (only manual `sync_source` tool calls).

The daemon updates `source_state` after each tick with the sync
result (success/failure, consecutive failures, timestamps). After
recording the sync result, it evaluates notification conditions and
dispatches any that fire (see Notifications below).

## Rebuild index

To regenerate the index after a parser fix or schema change, call
`rebuild_index`. It clears `source_documents` (cascading to
observations, problems, and medications) and re-ingests every
archived blob as CCDA, skipping non-CCDA blobs. Adapter data
(Fitbit heart-rate samples, Oura sleep stages) is not replayed;
run `sync_source` after to re-pull.

## Notifications

The notification system detects operational problems (auth failures,
sustained sync errors) and logs them so the user can discover issues
via the `list_notifications` MCP tool.

Two conditions are evaluated after every sync tick:

- **`auth_expired:{adapter}`** — fires with severity `"critical"` when
  the adapter's most recent error reason is `reauth_required`.
- **`sync_failures:{adapter}`** — fires with severity `"warning"` when
  the adapter has >= 3 consecutive sync failures.

Re-fire cadence: once a condition fires, the same condition is
suppressed for 24 hours unless it transitions to resolved and fires
again. State is tracked in the `notification_state` table; fired
notifications are appended to `notification_log`.

Architecture: the evaluator (`notifications/evaluator.rs`) is pure
(no async, no database) — it maps adapter state snapshots to
condition evaluations. The dispatcher (`notifications/dispatch.rs`)
handles fire-or-skip logic and writes to the database. This split
keeps the condition logic easy to unit-test.

## Confidence tracking

Sync decides which days to fetch by separating two distinct questions:

- **Coverage** — "do we already have this day?" A day is covered when it
  has a `source_day_state` row (we've successfully ingested it).
- **Settledness** — "might this day still change?" This is the per-adapter
  confidence model: `Confirmed` (settled) or `Provisional` (may change).

**The general fetch rule (every adapter follows this):** fetch a day if it
is **uncovered OR unsettled**; skip it only when it is **covered AND
settled**. This is `select_fetch_dates` in `sources/confidence.rs`. The rule
keeps backfill correct — a never-fetched historical day is always pulled
regardless of age — while avoiding redundant re-pulls of old, stable days.
Ingestion-time dedup (the UNIQUE-constraint catch) is a safety net, not the
primary correctness mechanism.

> Why this matters: an earlier Oura bug gated fetching on settledness alone.
> Because Oura settledness is purely time-based, every day older than ~24h
> was "settled" and skipped — so backfill silently fetched only the last
> day. Folding coverage into the rule fixed it.

**Fitbit settledness (stability-based):** A day is confirmed when it's
outside the 5-day force-refresh window AND the freshness frontier has passed
it (with 36h buffer) AND two consecutive pulls returned the same sample count
(`samples_count == samples_count_prev`). Note Fitbit already satisfied the
general rule before it was named — its confidence returns `Provisional` when
there's no `source_day_state` row, i.e. uncovered days are never skipped.

**Oura settledness (time-based):** A day is confirmed 24 hours after it ends.
Sleep data settles quickly; no stability check needed. Oura's sync uses
`select_fetch_dates` to combine this with coverage.

Confidence functions are pure (no async, no database) and live in each
adapter's `confidence.rs`. Shared types (`DayConfidence`, `ConfidenceByDate`),
`enumerate_dates`, and `select_fetch_dates` live in `sources/confidence.rs`.
