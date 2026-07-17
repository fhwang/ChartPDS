# Issue 27: Episode Bucketing, Aligned Tables, Two-Signal Relationships — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Answer routine per-episode and two-signal analysis questions with MCP tool calls alone: episode-based bucketing on the existing aggregation tools, a new aligned multi-coding table tool, and a new two-signal relationship (correlation) tool.

**Architecture:** A new pure episode-detection primitive (`queries/episodes.rs`) chains a coding's interval observations into episodes (gap-tolerant runs) and is reused by the three existing aggregation queries (which each gain an `episode` bucket) and by the new `aligned_table` query. The aligned-table column machinery (fetch one coding's rows, bucket by calendar unit or episode, aggregate to one value per bucket) is shared with the new `signal_relationship` query, which pairs two columns with an optional lag and computes Pearson r.

**Tech Stack:** Rust stable, sqlx (offline mode — run `just prepare-sql` after new SQL), jiff for timezone math, rmcp for the MCP tool surface.

## Global Constraints

- `just check` must pass: fmt, clippy `-D warnings` (every `pub` item needs a doc comment), tests, cargo deny/machete, `cargo sqlx prepare --check`, holdout-verify.
- Never bypass a lint; `#[allow]` requires `reason = "..."`.
- After any new/changed `sqlx::query!`: `just prepare-sql`, commit `.sqlx/` with the code.
- Holdout files are drafted only because the user explicitly asked; they are left **staged but uncommitted** and handed to the human to bless. Never run `just holdout-bless`; never edit existing holdout files.
- New public items exposed to the binary go through `crates/chartpds-core/src/lib.rs`? — No: queries are re-exported via `queries/mod.rs` and the binary reaches them as `chartpds_core::queries::…` (check `lib.rs` re-export of `queries` module; follow the existing pattern used by `observation_stats`).
- Episode bucket keys are RFC 3339 UTC instants of the episode's first interval start (e.g. `2026-06-27T02:00:00Z`) — chronological string sort order.

## Key domain facts (verified in-repo)

- Oura per-epoch sleep stages: coding `{system: https://chartpds.fhwang.net/coding/aasm/sleep-stage, code: aasm-sleep-stage}`, 5-min intervals, `value_quantity` = AASM discriminant (wake=0, n1=1, n2=2, **n3(deep)=3**, rem=4). Oura's `sleep_phase_5_min` chars: `1`=deep→n3, `2`=light→n2, `3`=REM→rem, `4`=awake→wake.
- Nightly summaries are interval observations spanning the sleep session: LOINC `93832-4` (total sleep, min) and `103215-0` (WASO, min), `value_unit: "min"`.
- Epochs within one session are contiguous (`next.start == prev.end`), so `gap_seconds = 0` chains a whole night into one episode; hours-long gaps separate nights.
- Holdout adapter-data path: `seed_archive_from_fixtures(subdir)` plants blob + `.meta.json` sidecar into the archive, then the `rebuild_index` tool replays them. Blob filename MUST be the SHA-256 hex of its bytes and the sidecar `id` must equal it.

---

### Task 1: Episode detection primitive (`queries/episodes.rs`)

**Files:**
- Create: `crates/chartpds-core/src/queries/episodes.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs` (add `mod episodes;` — internal, no `pub use` needed yet)

**Interfaces (Produces):**

```rust
/// One detected episode: a gap-tolerant chain of interval observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Episode {
    pub(crate) start: OffsetDateTime, // first interval's start
    pub(crate) end: OffsetDateTime,   // max end seen in the chain
}

/// Chain start-ordered intervals into episodes. Consecutive intervals join
/// while `next.start - current_end <= gap_seconds`; `current_end` advances
/// by max() so overlapping rows can't shrink the envelope.
pub(crate) fn detect_episodes(
    intervals: &[(OffsetDateTime, OffsetDateTime)],
    gap_seconds: i64,
) -> Vec<Episode>

/// Index of the episode containing `ts` (inclusive bounds: start <= ts <= end).
pub(crate) fn episode_index_for(episodes: &[Episode], ts: OffsetDateTime) -> Option<usize>

/// Infallible RFC 3339 UTC formatting (`YYYY-MM-DDTHH:MM:SSZ`) for bucket keys.
pub(crate) fn utc_instant_key(ts: OffsetDateTime) -> String
```

**Steps:**
- [ ] Write unit tests: single chain; gap splits; gap_seconds bridges; overlapping intervals keep max end; `episode_index_for` inclusive at both bounds, None outside; `utc_instant_key` formats a known instant (including non-UTC input offset normalized to Z).
- [ ] Implement; `cargo test -p chartpds-core queries::episodes`.
- [ ] Commit.

### Task 2: `episode` bucket in `duration_in_value_range` + tool arg

**Files:**
- Modify: `crates/chartpds-core/src/queries/duration_in_value_range.rs`
- Modify: `crates/chartpds-mcp/src/server.rs` (`ObservationDurationInRangeArgs`, tool fn, description)

**Interfaces:**
- `Bucket` gains `Episode` variant.
- `DurationInValueRangeParams` gains `pub gap_seconds: i64` (doc: episode chaining tolerance; ignored by other buckets). All existing call sites pass `0`.
- Episode path: new SQL fetching ALL interval rows of the coding in the window (no value filter) joined to `source_documents` for `(source, document_date)`; detect episodes over all rows; per episode sum minutes of rows with `value_min <= value <= value_max`. **Every** episode yields a `BucketMinutes` row (0.0 when nothing in range); `bucket_start = utc_instant_key(episode.start)`. Confidence: contributions `(label, source, document_date)` for all rows in the episode → `roll_up_bucket_confidence`.
- Server: `bucket: "episode"` accepted; new optional `gap_seconds` arg (default 0, invalid_params if negative).

**Steps:**
- [ ] Core tests: two sleep-night-shaped groups of contiguous epochs (one crossing UTC midnight) → two episodes, midnight-crossing night lands wholly in one bucket keyed by its start instant; episode with zero in-range minutes still reported as 0.0; gap_seconds bridges an intra-night data gap.
- [ ] Implement episode path; `just prepare-sql`; `cargo test -p chartpds-core`.
- [ ] Server: accept `"episode"`, wire gap_seconds; server test via direct construction (pattern at `server.rs` bottom) asserting bucket keys are RFC3339 instants.
- [ ] Commit (code + `.sqlx/`).

### Task 3: `episode` bucket in `longest_continuous_in_value_range` + tool arg

**Files:**
- Modify: `crates/chartpds-core/src/queries/longest_continuous_in_value_range.rs`
- Modify: `crates/chartpds-mcp/src/server.rs` (`ObservationLongestPeriodInRangeArgs` bucket doc + parsing)

**Interfaces:**
- `LongestContinuousParams` gains `pub bucket: LongestBucket` where `pub enum LongestBucket { Day, Episode }` (exported). Existing call sites pass `Day`.
- Episode path: fetch all intervals (new unfiltered SQL, same shape as Task 2's) → episodes with `gap_seconds`; existing in-range SQL → `runs(intervals, gap_seconds)`; attribute each run to the episode containing its start; per episode report longest run (0.0 rows for run-less episodes); `bucket_start = utc_instant_key(episode.start)`.
- Reuse Task 2's fetch by making it a `pub(crate) fn fetch_all_intervals(pool, coding_system, coding_code, start, end) -> sqlx::Result<Vec<IntervalRow>>` in `episodes.rs` (one query, one `.sqlx` entry) where `IntervalRow { start, end, value: Option<f64>, source: String, document_date: Option<String> }`.

**Steps:**
- [ ] Core tests: in-range runs split per episode; a run crossing UTC midnight attributed wholly to its episode; empty episode → 0.0.
- [ ] Implement; `just prepare-sql`; tests.
- [ ] Server: accept `"episode"` bucket; test.
- [ ] Commit.

### Task 4: `episode` bucket in `observation_stats` + tool arg

**Files:**
- Modify: `crates/chartpds-core/src/queries/observation_stats.rs`
- Modify: `crates/chartpds-mcp/src/server.rs` (`ObservationStatsArgs` + parsing)

**Interfaces:**
- `StatsBucket` gains `Episode`; `ObservationStatsParams` gains `pub gap_seconds: i64`.
- Bucket assignment for `Episode`: build intervals from fetched rows as `(effective_start, effective_end.unwrap_or(effective_start))` (already start-ordered by SQL), `detect_episodes(…, gap_seconds)`, label rows by containing episode via `episode_index_for` + `utc_instant_key`. Sort index stays 0 (keys sort chronologically).
- Server: accept `"episode"` for `bucket`, new optional `gap_seconds`.

**Steps:**
- [ ] Core tests: interval_minutes stats grouped per episode (two nights → two buckets); point rows (no end) each form their own episode with gap 0.
- [ ] Implement; tests pass (no new SQL).
- [ ] Server parsing + test; commit.

### Task 5: Aligned multi-coding table core (`queries/aligned_table.rs`)

**Files:**
- Create: `crates/chartpds-core/src/queries/aligned_table.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs` (mod + `pub use aligned_table::{aligned_table, AlignedTable, AlignedTableError, AlignedTableParams, ColumnAggregate, ColumnSpec, EpisodeSpec, TableBucket, TableRow};`)
- Modify: `crates/chartpds-core/src/queries/observation_stats.rs` (make `bucket_key`, `field_value` + row type needs `pub(crate)` access, or re-derive locally — prefer making `bucket_key` and a standalone `field_value(effective_start, effective_end, value_quantity, field, tz)` helper `pub(crate)`)

**Interfaces (Produces):**

```rust
pub enum ColumnAggregate { Mean, Sum, Min, Max, Count, Median, DurationInRange { value_min: f64, value_max: f64 } }
pub struct ColumnSpec<'a> { pub coding_system: &'a str, pub coding_code: &'a str, pub aggregate: ColumnAggregate, pub field: StatsField }
pub enum TableBucket { Day, Week, Month, Episode }
pub struct EpisodeSpec<'a> { pub coding_system: &'a str, pub coding_code: &'a str, pub gap_seconds: i64 }
pub struct AlignedTableParams<'a> {
    pub columns: &'a [ColumnSpec<'a>],
    pub start: OffsetDateTime, pub end: OffsetDateTime,
    pub bucket: TableBucket, pub episode: Option<EpisodeSpec<'a>>,
    pub timezone: Option<&'a str>,
}
pub struct TableRow { pub bucket_key: String, pub values: Vec<Option<f64>>, pub confidence: DayConfidence }
pub struct AlignedTable { pub rows: Vec<TableRow> }  // serde::Serialize on all
pub enum AlignedTableError { Db(sqlx::Error), InvalidTimezone(String), MissingEpisodeSpec, Internal(String) } // thiserror
pub async fn aligned_table(pool: &SqlitePool, now: OffsetDateTime, params: AlignedTableParams<'_>) -> Result<AlignedTable, AlignedTableError>
```

Semantics (the contract the tests lock):
- Row set: `Episode` → one row per detected episode of the episode coding (even if all values null); calendar buckets → union of buckets where ≥1 column has data. Chronological order.
- Cell value: `DurationInRange` → null when the coding has **no** interval rows in the bucket, else in-range minutes (0.0 possible). Other aggregates → aggregate over present field values, null when none; `Count` counts rows with the field present (null when no rows at all — a bucket the coding never touched reads null, not 0).
- Episode assignment for **all** columns: `effective_start` contained in episode (inclusive bounds).
- Confidence per row rolled up across every contributing row of every column.
- Also produce `pub(crate) fn column_cells(...)` (per-column `BTreeMap<String, CellAcc>` + contributions) reused by Task 7 — exact split may be adjusted during implementation, but signal_relationship must NOT duplicate the fetch/bucket/aggregate logic.

**Steps:**
- [ ] Core tests (seed via `test_support` + direct index inserts for multi-coding): two codings per-day alignment with an explicit null; duration_in_range column (elevated-HR minutes) alongside value columns; episode mode with sleep epochs defining episodes and nightly summaries aligned into them; missing episode spec → `MissingEpisodeSpec`; invalid timezone error; median/count/sum/min/max aggregates.
- [ ] Implement; `just prepare-sql` (reuses Task 3's `fetch_all_intervals`? No — columns need value rows incl. NULL ends: one new query fetching `(effective_start, effective_end?, value_quantity?, source, document_date)` per coding — this is exactly `observation_stats`'s `fetch_rows`; make that `pub(crate)` and reuse it instead of adding SQL).
- [ ] Commit.

### Task 6: `observation_table` MCP tool

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs`

**Args (JsonSchema structs):**

```rust
struct TableColumnArgs { coding: Coding, aggregate: Option<String>, field: Option<String>, value_min: Option<f64>, value_max: Option<f64> }
struct TableEpisodeArgs { coding: Coding, gap_seconds: Option<i64> }
struct ObservationTableArgs { columns: Vec<TableColumnArgs>, start: String, end: String, bucket: Option<String>, timezone: Option<String>, episode: Option<TableEpisodeArgs> }
```

Validation → `invalid_params`: empty columns; unknown aggregate/field/bucket; `duration_in_range` without both value bounds; value bounds with any other aggregate; `bucket:"episode"` without `episode`; `episode` with a non-episode bucket. Default aggregate `mean`, field `value`, bucket `day`.

Response: `{"columns":[{"system","code","aggregate","field"}], "rows":[{"bucket_key","values":[…nulls…],"confidence"}]}` — columns echoed in request order so clients map values positionally.

**Steps:**
- [ ] Server tests: end-to-end alignment with null; episode table; validation rejections.
- [ ] Implement tool with `#[tool(description = …)]` documenting the issue-27 example ("one row per day with total sleep, WASO, minutes of elevated HR").
- [ ] Commit.

### Task 7: Two-signal relationship core (`queries/signal_relationship.rs`)

**Files:**
- Create: `crates/chartpds-core/src/queries/signal_relationship.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs` (mod + `pub use signal_relationship::{signal_relationship, GroupSummary, RelationshipBucket, RelationshipGroups, SignalRelationship, SignalRelationshipParams};`)

**Interfaces (Produces):**

```rust
pub enum RelationshipBucket { Day, Week, Month }
pub struct SignalRelationshipParams<'a> {
    pub x: ColumnSpec<'a>, pub y: ColumnSpec<'a>,
    pub start: OffsetDateTime, pub end: OffsetDateTime,
    pub bucket: RelationshipBucket, pub timezone: Option<&'a str>,
    pub lag_buckets: i64,            // pair x@t with y@(t+lag)
    pub x_threshold: Option<f64>,    // optional group comparison on x
}
pub struct GroupSummary { pub count: usize, pub mean: Option<f64>, pub sd: Option<f64>, pub min: Option<f64>, pub max: Option<f64>, pub p50: Option<f64> }
pub struct RelationshipGroups { pub x_below: GroupSummary, pub x_at_or_above: GroupSummary } // y-stats grouped by x vs threshold (strictly-below convention, matching observation_stats thresholds)
pub struct SignalRelationship {
    pub n_pairs: usize,
    pub pearson_r: Option<f64>,      // null when n<2 or either sd is 0
    pub x_mean: Option<f64>, pub x_sd: Option<f64>,
    pub y_mean: Option<f64>, pub y_sd: Option<f64>,
    pub groups: Option<RelationshipGroups>,
}
pub async fn signal_relationship(pool, now, params) -> Result<SignalRelationship, AlignedTableError>
```

- Pairing: compute both columns' per-bucket values via Task 5's shared machinery; for each x bucket key `k` with a value, look up y at `shift_bucket_key(k, bucket, lag_buckets)`; both present → pair. Buckets missing either signal are excluded and `n_pairs` reflects only kept pairs.
- `shift_bucket_key`: Day → date + lag days; Week → Monday + 7·lag days; Month → year/month arithmetic (`YYYY-MM`). Pure, unit-tested (incl. negative lag, year boundaries).
- Pearson: `Σ(dx·dy) / sqrt(Σdx²·Σdy²)`; means/sds are over the paired samples (n−1 sd).

**Steps:**
- [ ] Unit tests for `shift_bucket_key` and a pure `pearson(xs, ys)` helper (exact hand-checkable sets: x=(1,2,3), y=(1,3,2) → 0.5; constant y → None).
- [ ] Core async tests: two codings daily → n_pairs/r; lag 1 pairs day t with t+1 and drops unmatched edges; missing-day exclusion; threshold groups (strictly-below split) with exact group means.
- [ ] Implement; tests; commit.

### Task 8: `observation_relationship` MCP tool

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs`

Args: `x: TableColumnArgs`, `y: TableColumnArgs`, `start`, `end`, `bucket: Option<String>` (`day` default / `week` / `month`), `timezone: Option<String>`, `lag: Option<i64>` (default 0), `threshold: Option<f64>`. Same column validation as Task 6. Response: serialized `SignalRelationship`.

**Steps:**
- [ ] Server tests: end-to-end exact r; lag; threshold groups; validation.
- [ ] Implement tool (description names the "activity by day vs sleep the following night → lag 1" pattern); commit.

### Task 9: Documentation + full gate

**Files:**
- Modify: `CLAUDE.md` (queries list; MCP server tool list 16→18 with `observation_table` + `observation_relationship`; episode bucket mentions on the three grown tools)

**Steps:**
- [ ] Update docs; run `just check` (expect green except possibly holdout-verify once Task 10 lands — at this point it must be fully green).
- [ ] Commit.

### Task 10: Draft holdout tests (explicitly requested — leave staged, uncommitted)

**Files (all new — never touch existing holdout files):**
- Create: `holdout/fixtures/episode_sleep_nights/<sha256>` + `<sha256>.meta.json` (×2 nights, Oura `oura-sleep-session` JSON, same shape as `holdout/fixtures/oura_sleep_night/`; night A crosses UTC midnight and contains a known count of `1` (deep) epochs; night B a different count)
- Create: `holdout/fixtures/relationship_vitals.xml` (CCDA vitals: body weight 380/400/420 on Jan 1/2/3 02:16Z + heart rate 60/80/70 same days — modeled on `holdout/fixtures/observation_stats_vitals.xml`; weight also on Jan 4 with **no** HR that day for the null-cell lock)
- Create: `holdout/tests/episode_bucketing.rs`, `holdout/tests/aligned_table.rs`, `holdout/tests/signal_relationship.rs`

**Contracts to lock:**
1. `episode_bucketing.rs` — seed oura fixtures + `rebuild_index`; `observation_duration_in_range {aasm coding, value 3..3, bucket:"episode"}` → exactly one row per night keyed by the episode-start instant; the midnight-crossing night's deep minutes land wholly in one bucket (assert exact minutes per night; assert bucket count == 2).
2. `aligned_table.rs` — ingest CCDA; `observation_table` day-bucketed with weight + HR columns → one row per day, positional values, explicit `null` for the HR-less day; single tool call, no client joining.
3. `signal_relationship.rs` — `observation_relationship` weight-vs-HR daily: `n_pairs == 3`, `pearson_r == 0.5` exactly (hand-checkable: devs (−20,0,20)×(−10,10,0) → 200/400); with `lag:1`: `n_pairs == 2`, `pearson_r == -1.0` (unmatched edge days excluded); threshold group means exact.
- Blob filenames: `shasum -a 256` of the exact bytes; sidecar `id` equal to it, `type:"oura-sleep-session"`, `subject` = the Oura day, fixed `time`.

**Steps:**
- [ ] Build fixtures (compute hashes), write tests, `cargo test -p chartpds-holdout` — all new tests green against the implemented tools.
- [ ] `git add` the new holdout files; leave uncommitted; report "ready to bless" to the human (holdout-verify will flag the staged files until blessed — expected, do not regenerate the lock).

### Task 11: Finish branch
- [ ] `just check`; verify only the expected holdout-verify caveat remains (if any).
- [ ] Commit any stragglers, push branch, open PR referencing issue 27 (implementation only; note the staged holdout drafts awaiting bless). Author as Francis Hwang <sera@fhwang.net>; no personal data in the PR.

## Self-review notes
- Spec coverage: capability 1 → Tasks 1–4 (all three day-bucketing aggregation tools gain episode mode; example query = Task 2 + holdout 1). Capability 2 → Tasks 5–6 (example = sleep/WASO/elevated-HR columns; explicit nulls; episode mode "where applicable"). Capability 3 → Tasks 7–8 (n_pairs + Pearson; lag covers "following period"; nice-to-have threshold groups included; missing pairs excluded).
- Type consistency: `ColumnSpec`/`StatsField` shared between Tasks 5–8; `Episode`/`utc_instant_key` shared between Tasks 1–5; `LongestBucket` only in Task 3.
