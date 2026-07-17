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

The index currently has 10 tables: `source_documents`, `observations`,
`problems`, `medications`, `narrative_texts` (populated by ingestion),
`source_credentials`, `source_state`, `source_day_state` (populated by
the adapter/sync layer), plus `notification_state` and `notification_log`
(populated by the notification system). `narrative_texts` has an FTS5
companion index, `narrative_texts_fts` (external-content, trigger-maintained
on insert/update/delete of `narrative_texts`), used by `search_narratives`.

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

## Archive

The archive (`archive/`) is a content-addressed blob store: every blob's
path is the SHA-256 hex of its bytes, so equal content dedupes automatically.
All source types share one flat `$DIR/archive/` directory; the bytes stay the
raw, untouched payload (CCDA XML, Fitbit/Oura JSON, clinical PDFs).

Each blob is paired with a **sidecar manifest** `<hash>.meta.json` that makes
the archive self-describing — the SQLite index can be fully reconstructed from
the archive alone. The manifest follows the
[CloudEvents v1.0](https://github.com/cloudevents/spec) context-attribute model
in *binary content mode* (`archive/manifest.rs`): `type` (== `source_documents.kind`),
`source`, `datacontenttype`, `subject` (the per-document replay date — Fitbit
day / Oura sleep day — so no adapter needs a custom blob envelope), `time`
(== `archived_at`, immutable), and `originalfilename`.

Write blobs with `Archive::put_with_manifest(content, manifest)`; it writes the
blob then the sidecar and stamps the manifest `id` with the content hash. Bare
`Archive::put` (no manifest) still exists for tests. `list_keys` filters to
64-char hex, so `.meta.json` sidecars are never mistaken for blobs.

`archived_at` is the immutable time the bytes first entered the archive. It is
carried in the manifest and copied *through* on rebuild — it is **not** the
projection-build time and must never be re-stamped to "now".

## Derived store

`$DIR/derived/` is a second content-addressed blob store (same `Archive` type,
same manifest sidecar scheme) holding **machine-generated derivations** —
currently the frozen narrative extraction artifacts. The three storage tiers:

- **archive** — bytes that arrived from outside. Sacred; never GC'd; the
  system of record.
- **derived** — expensive-to-recreate derivations (LLM output is
  non-deterministic and costs money to regenerate, so it is persisted, not
  recomputed on rebuild). Versioned via each artifact's `extractor`
  `{model, prompt_version}`; less sacred than the archive, more precious than
  the index.
- **index** — the disposable SQLite projection, rebuilt from the two stores.

`rebuild_index` replays both stores. Derived-store blobs must be
`narrative-extraction` artifacts; anything else there is skipped, never
type-sniffed as a source document. Extraction artifacts found in the
*archive* (a legacy layout predating the derived store) still replay
identically.

## Ingestion

`ingestion::ingest()` is the canonical write path: archive blob + manifest ->
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

## Narrative documents

`kind = "clinical-pdf"` ingests a narrative clinical PDF (pathology/imaging
report, visit note) instead of structured CCDA. `ingestion::ingest_narrative_pdf`
is the orchestrator: deterministic text extraction (`extraction::extract_pdf_text`,
via `pdf-extract`) runs first and unconditionally — a scanned PDF with no text
layer is a hard error (`Error::NoTextLayer`) before anything is archived;
OCR is out of scope. A one-time LLM pass
(`extraction::ClaudeExtractor`, model pinned in `extraction/llm.rs` as
`EXTRACTION_MODEL`) then extracts explicitly-quoted ICD-10-CM codes and a
document date; it requires `ANTHROPIC_API_KEY` — there is no text-only
fallback (see the failure-handling paragraph below). `ANTHROPIC_BASE_URL` optionally overrides the API endpoint (same
variable the official Anthropic SDKs honor — for proxies/gateways, and how
the holdout suite points the binary at a local mock LLM server). LLM output is never trusted directly: `extraction::verify_extraction`
mechanically checks every claim against the extracted text (whitespace-
normalized substring match; a coding's code must appear inside its own quote;
a claimed date must appear literally in its quote) and drops anything that
doesn't verify, recording why in `rejected`.

Design invariant: the LLM runs exactly once, at ingest. Its verified output is
frozen as its own JSON blob in the **derived store** (`$DIR/derived/`) — an
`ExtractionArtifact` (manifest kind `NARRATIVE_EXTRACTION_KIND` =
`"narrative-extraction"`) referencing the PDF blob's hash — and
`rebuild_index` replays that frozen artifact rather than calling the model
again. This keeps rebuilds free, deterministic, and safe to run without
network access or an API key.

Verified codings land in `problems` alongside CCDA-derived ones, with
`problems.section_label` set to the verbatim section heading the code
appeared under in the source document (NULL for CCDA rows — it's
presentational provenance for an LLM reader, not machine-aggregatable).

LLM extraction is required: if it cannot run — for any reason — the whole
ingest fails before anything is archived or indexed. A missing
`ANTHROPIC_API_KEY` errors immediately (`Error::ExtractorNotConfigured`,
naming the fix). A transient LLM failure is retried in-band by
`ClaudeExtractor` (3 attempts total; connection errors and HTTP 429/5xx,
with short linear backoff — see `MAX_ATTEMPTS` in `extraction/llm.rs`);
a sustained outage then errors the `ingest_record` call. In every failure
case nothing is persisted, so there is no partial text-only state to
reconcile and nothing for `rebuild_index` to resurrect — the caller fixes
the configuration or waits out the outage, then re-runs the same ingest.
(Total *verification* failure is not an error: an extraction where every
claim fails verification still applies as `"applied"` with zero codings
and the reasons in `rejected`.)

## Queries

`queries::` holds generic analytical primitives over the index.
Currently: `latest_by_code`, `observation_history`, `counts_per_code`,
`current_problems`, `current_medications`, `duration_in_value_range`,
`longest_continuous_in_value_range`, `search_narratives`, `get_narrative`,
`observation_stats`, `aligned_table`, `signal_relationship`. Each is a pure async function
`(&SqlitePool, args) -> Result<T, sqlx::Error>`;
there is no shared state and no struct-style query builder.
The `current_problems` and `current_medications` primitives return deduped
lists with per-code provenance, replacing the earlier `list_problems` and
`list_medications` functions.
`observation_history` accepts multiple `{system, code}` codings with optional
open-ended bounds, replacing the earlier `in_range` function.
`counts_per_code` returns per-`(system, code)` summaries including
`count`, `first_effective_start`, and `last_effective_start`.
Both `duration_in_value_range` and `longest_continuous_in_value_range`
select observations by `{coding_system, coding_code}`; day bucketing uses
the UTC calendar day of the interval or run start. They attribute that day
differently, and the difference is intentional: `duration_in_value_range`
buckets each interval independently (a run crossing UTC midnight has its
minutes split across both days), while `longest_continuous_in_value_range`
attributes a whole run to the UTC day its first interval started (a
midnight-crossing block lands wholly in one day). So per-day totals from the
two tools need not reconcile for a block that straddles midnight.

`duration_in_value_range`, `longest_continuous_in_value_range`, and
`observation_stats` also accept an `episode` bucket: episodes are
gap-tolerant chains of the coding's interval observations (detection lives
in `queries/episodes.rs`), keyed by the episode's RFC 3339 UTC start
instant, so a sleep period crossing midnight lands in exactly one bucket.
`aligned_table` returns one row per bucket (day / ISO week / month /
episode) with one value per requested coding — each column reduces via
mean / sum / min / max / count / median or in-range interval minutes, and
absent cells are explicit nulls. `signal_relationship` pairs two codings'
per-bucket values with an optional lag (in buckets) and reports `n_pairs`,
Pearson r, Spearman ρ (rank-based; robust to outliers and
monotonic-but-nonlinear relationships), per-signal mean/sd, and optional
per-group `y` statistics split by an `x` threshold.

These are composed into named MCP tools (and later CLI subcommands)
by the binary crate. Adding a new query: drop a new file under
`crates/chartpds-core/src/queries/`, add a `mod` declaration and a
`pub use` re-export in `queries/mod.rs`, run `just prepare-sql` to
cache the new SQL.

## MCP server

`chartpds-mcp` is the stdio MCP server binary. It reads
`CHARTPDS_DATA_DIR` from the environment (a plain absolute directory path,
e.g. `/path/to/chartpds-data`), opens the SQLite pool at `$DIR/chartpds.db`,
the local-FS archive at `$DIR/archive/`, and the derived store at
`$DIR/derived/`, and serves 18 tools:

- `ingest_record` — ingest a document (write path); `kind="ccda"` for CCDA
  XML, `kind="clinical-pdf"` for a narrative clinical PDF (see Narrative
  documents below) — indexes its text and extracts verified codings; requires
  `ANTHROPIC_API_KEY` (a keyless or failed extraction fails the ingest)
- `latest_observation_by_code` — most-recent observation by LOINC code
- `get_observation_history` — observations across one or more `{system, code}` codings,
  with optional open-ended `since`/`until` bounds (replaces `observations_in_range`)
- `observation_counts` — discover codings present in the store: returns
  `{coding_system, coding_code, count, first_effective_start, last_effective_start}`
  per `(system, code)`
- `describe_codings` — value-encoding semantics for the codings ChartPDS mints
  (non-standard only; LOINC omitted as self-describing)
- `observation_duration_in_range` — total minutes a coded signal spent in a
  value range; buckets: `none` / `day` / `hour` / `episode`
- `observation_longest_period_in_range` — longest continuous in-range run
  per day or per episode
- `observation_stats` — descriptive statistics (count, mean, sample sd,
  min/max, p25/p50/p75, optional threshold counts) for one coding over a
  window; `field` selects `value`, `start_time_of_day` / `end_time_of_day`
  (minutes since local noon), or `interval_minutes`; optional bucketing by
  `day` / `week` (ISO Monday) / `month` / `day_of_week` / `episode` in a
  request timezone
- `observation_table` — aligned multi-coding table: one row per bucket
  (`day` / `week` / `month` / `episode`) with one aggregated value (or
  explicit null) per requested coding, in a single call
- `observation_relationship` — how two codings relate over a window:
  per-bucket pairing with optional lag (in buckets), Pearson r, Spearman ρ,
  `n_pairs`, and optional threshold-group comparison
- `list_problems` — current problems (diagnoses), deduped to one entry per
  code with provenance (`document_count`, `first_seen`, `last_seen`) and the
  archive's `latest_document_date`; `status` is the raw source value and is
  unreliable for active/resolved
- `list_medications` — current medications, deduped per code with the same
  provenance shape
- `connect_source` — connect a data source (fitbit OAuth or oura PAT)
- `sync_source` — sync one or all sources; returns `{results:[{source, ok,
  days_synced?, total_samples?, reason?, message?}]}` with failures reported
  in-band (reason ∈ reauth_required, no_credentials, transient, parse_error,
  archive_error, database_error)
- `rebuild_index` — drop and rebuild the index from archived blobs
- `list_notifications` — list recent notification log entries (newest first)
- `search_narratives` — full-text search (FTS5, BM25-ranked) over narrative
  clinical documents; omit `query` to list the whole narrative catalog
  newest-first
- `get_narrative` — fetch one narrative document by `source_document_id`:
  metadata, full extracted text, and its verified codings (with
  `section_label`)

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

For each `long_sleep` night the adapter also emits two nightly summary
observations: a total-sleep observation (LOINC `93832-4`, value in minutes)
and a wake-after-sleep-onset (WASO) observation (LOINC `103215-0`, minutes
awake after sleep onset), derived from the epoch stream. These land alongside
the per-epoch `aasm-sleep-stage` observations in the `observations` table.

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
observations, problems, medications, and narrative_texts) and replays
every blob from both the archive and the derived store, routed by its
sidecar manifest `type`: CCDA documents are re-ingested, Fitbit/Oura
blobs are replayed by their adapters
(`sources::{fitbit,oura}::storage::replay`), and `clinical-pdf`
blobs have their text re-derived deterministically (`extract_pdf_text`
on the archived bytes — never an LLM call). All sources are
reconstructed from the two stores alone — no `sync_source` re-pull is
needed. Each blob's `archived_at` is preserved from its manifest.

Narrative extraction results are replayed, not regenerated: rebuild runs
in two phases so a `narrative-extraction` artifact blob (normally in the
derived store; legacy artifacts in the archive replay identically) is
collected in phase one and applied to its narrative document's `problems`
rows in a second pass once every document exists. This is what makes
rebuild network-free even for narrative documents extracted with an LLM.

Blobs with no manifest (legacy, pre-manifest) fall back to a
best-effort CCDA parse. Unknown manifest types and malformed payloads
are counted as skipped. `RebuildResult` reports per-source counts:
`{blobs_found, ccda_ingested, fitbit_ingested, oura_ingested,
blobs_skipped, narratives_ingested, extractions_applied}`.

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
