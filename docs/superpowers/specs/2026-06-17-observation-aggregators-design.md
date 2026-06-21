# P0: Periodic-signal aggregators + nightly sleep duration

*Date: 2026-06-17. Status: approved design, pre-implementation.*

## Motivation

An external MCP client (a weekly health-summary routine) needs three
capabilities the current tool surface does not provide. All three concern
*aggregating* periodic observations that today are only queryable as raw
points/intervals:

1. **Total time a periodic signal spent inside a value range** — e.g.
   minutes of heart rate in a target zone over a week (MVPA). Raw heart
   rate is in the index (>1M samples) but only `observations_in_range`
   exists, so a client would have to pull every sample and sum them — the
   exact cost the typed-tool design exists to avoid.
2. **A nightly total-sleep-duration observation** — the Oura adapter today
   emits only per-epoch sleep-stage rows, so "how long did the user sleep
   last night" requires counting epochs client-side.
3. **The longest unbroken run of in-range observations per day** — e.g. the
   longest continuous block of sleep per night.

These are the three items that block a tracked metric. Lower-priority
ergonomic requests (richer discovery, multi-coding history, document
listing, deduped current problems/medications, structured sync errors) are
explicitly **out of scope** for this spec and will be addressed separately.

## Key data fact

Both underlying signals are already stored as half-open
`[effective_start, effective_end)` **intervals**, not bare instants:

- Fitbit heart rate: `sources/fitbit/parser.rs` synthesizes an interval per
  sample from the gap to the next sample (capped at 90s → 60s).
- Oura sleep: each 5-minute epoch is a 300s interval.

So the aggregators do not invent durations. #1 sums interval lengths; #3
finds the longest unbroken chain of intervals. This also means the existing
`value_quantity` carries the comparable number for both signals (BPM for
heart rate, the AASM stage discriminant 0–4 for sleep).

## Cross-cutting: coding selection

The two new query primitives select observations by a full
`{coding_system, coding_code}` pair, not a bare LOINC code string. This is
required because #3 targets the non-LOINC AASM sleep-stage coding
(`https://chartpds.fhwang.net/coding/aasm/sleep-stage` / `aasm-sleep-stage`)
and #1 targets a LOINC code (`http://loinc.org` / `8867-4`). Both filter on
`coding_system = ? AND coding_code = ?`.

The existing `observations_in_range` / `in_range` query keeps its bare-code
signature unchanged; it is not part of this work.

## Architecture

The implementation split is driven by data volume.

### #1 `duration_in_value_range` — aggregate in SQL

Heart rate is the high-volume case (>1M rows). Summing must happen in
SQLite, returning a single number (or one per day), never shipping rows to
Rust.

New query primitive in `queries/`:

```
duration_in_value_range(
    pool, coding_system, coding_code,
    start, end,                 // half-open [start, end) on effective_start
    value_min, value_max,       // inclusive on value_quantity
    bucket,                     // Bucket::None | Bucket::Day
) -> DurationInRange
```

SQL: filter `coding_system = ? AND coding_code = ?`,
`effective_start >= ? AND effective_start < ?`,
`value_quantity BETWEEN ? AND ?`, `effective_end IS NOT NULL`. Sum
`(julianday(effective_end) - julianday(effective_start)) * 86400` seconds,
then convert to minutes.

- `Bucket::None` → `{ total_minutes: f64 }`.
- `Bucket::Day` → `{ per_bucket: [{ bucket_start, total_minutes }] }`,
  grouped by the **UTC calendar day of `effective_start`**
  (`date(effective_start)`), ordered ascending.

Rows with `effective_end IS NULL` contribute zero (they have no duration);
this tool is meaningful only for interval-shaped signals.

### #3 `longest_continuous_in_value_range` — fetch ordered, walk in Rust

Per-night volume is tiny (~200 epochs), and run/gap detection is awkward in
SQL. Fetch the qualifying rows ordered by `effective_start`, then hand the
intervals to a **pure** walker (no async, no DB — mirrors the
`sources/confidence.rs` and `notifications/evaluator.rs` pure-core pattern,
so the run logic is unit-testable in isolation).

New query primitive in `queries/`:

```
longest_continuous_in_value_range(
    pool, coding_system, coding_code,
    start, end,
    value_min, value_max,
    bucket,                     // Bucket::Day (the only mode the client uses)
    gap_seconds,                // allowed gap between consecutive in-range intervals
) -> LongestContinuousInRange { per_bucket: [{ bucket_start, longest_minutes }] }
```

Pure walker `longest_run(intervals, gap_seconds) -> f64` (minutes):

- Walk intervals in time order. Each is already known to be in range (the
  SQL filter removed out-of-range and out-of-window rows).
- Two consecutive intervals join into the same run when
  `next.start - prev.end <= gap_seconds`; a larger time gap starts a new run.
- Run length = **wall-clock span** of the chain: `last.end - first.start`.
  With sleep's contiguous 300s epochs and `gap_seconds = 0` this equals the
  summed durations; span is the natural reading of "longest continuous
  block."
- Returns the maximum run length.

`Bucket::Day` assigns each run to the **UTC calendar day of its start** and
reports, per day present, the longest run starting that day.

Because out-of-range rows are filtered out in SQL, a run break comes from
either a too-large time gap or a day with no qualifying rows. (Filtering
out-of-range rows server-side is what lets the walker treat every received
interval as in-range; gap tolerance then bridges only missing-data gaps, not
out-of-range interruptions — matching the client's stated intent.)

### Shared bucketing note

Day bucketing uses the UTC calendar day of the interval/run **start**. This
is a deliberate simplification: a sleep "day" in local time, or an interval
straddling midnight, is attributed wholly to its UTC start day. For the
client's use (total in-zone minutes over a trailing 7-day window, and
longest nightly sleep block) this is correct enough and far simpler than
local-time/midnight-splitting. Documented as a known limitation.

### #2 nightly sleep duration — Oura adapter only, no new tool

The nightly total is read through the **existing**
`observations_in_range({ code: "93832-4", ... })`; no query primitive or MCP
tool is added. The work is purely in ingestion/projection.

- Pure helper in `sources/oura/parser.rs`:
  `nightly_sleep_duration(session) -> Option<ParsedSleepDuration>`. Returns
  `Some` only when `session.session_type == "long_sleep"` **and**
  `session.total_sleep_duration` is `Some` (long-sleep-only; skip on null).
  The returned struct carries `effective_start` (= `bedtime_start`),
  `effective_end` (= `bedtime_end`), and `minutes` (= total seconds / 60).
- `sources/oura/storage.rs::index_sleep_session` inserts one extra
  observation after the per-epoch loop, when the helper returns `Some`:
  - `coding_system = http://loinc.org` (the existing `SYSTEM_LOINC` const)
  - `coding_code = 93832-4`, `coding_display = "Sleep duration"`
  - `effective_start = bedtime_start`, `effective_end = bedtime_end`
  - `value_quantity = total_sleep_duration / 60.0`, `value_unit = "min"`
- A new LOINC constant for `93832-4` lives in `clinical/`.
- `source_day_state.samples_count` stays the **per-epoch count**. The
  nightly row is a derived projection, not a raw sample, and the count
  feeds Fitbit's stability-based confidence model; leaving it unchanged
  keeps confidence semantics and existing tests intact.
- Rebuild: `index_sleep_session` is the shared write tail used by both live
  sync and `rebuild_index` replay, so nightly observations are produced on
  rebuild with no extra wiring.

## MCP tools

Two new tools on `ChartPdsServer` (`crates/chartpds-mcp/src/server.rs`),
following the existing `observations_in_range` handler shape. Names follow
house style (no `get_` prefix, consistent with `observations_in_range`,
`observation_counts`):

- **`observation_duration_in_range`** — wraps `duration_in_value_range`.
  Args: `coding { system, code }`, `start`, `end` (RFC 3339), `value_min`,
  `value_max`, `bucket` (`"none"` | `"day"`, default `"none"`). Returns the
  `DurationInRange` JSON.
- **`observation_longest_period_in_range`** — wraps
  `longest_continuous_in_value_range`. Args: `coding { system, code }`,
  `start`, `end`, `value_min`, `value_max`, `bucket` (`"day"`, default
  `"day"`), `gap_seconds` (default `0`). Returns the
  `LongestContinuousInRange` JSON.

#2 adds no tool.

## Files touched

Core:
- `crates/chartpds-core/src/queries/duration_in_value_range.rs` (new)
- `crates/chartpds-core/src/queries/longest_continuous_in_value_range.rs`
  (new; contains the pure `longest_run` walker + its unit tests)
- `crates/chartpds-core/src/queries/mod.rs` (mod + re-exports)
- `crates/chartpds-core/src/clinical/` (new `93832-4` LOINC constant +
  re-export)
- `crates/chartpds-core/src/sources/oura/parser.rs` (nightly helper)
- `crates/chartpds-core/src/sources/oura/storage.rs` (insert nightly obs)
- `crates/chartpds-core/src/lib.rs` (re-exports if needed for the binary)

Binary:
- `crates/chartpds-mcp/src/server.rs` (two new tools + arg structs + tests)

SQL cache:
- `.sqlx/` regenerated via `just prepare-sql` (two new queries), committed
  with the code.

## Shared types

`Bucket` enum (`None` / `Day`) and the small result structs
(`DurationInRange`, `LongestContinuousInRange`, `BucketMinutes`) live with
the query primitives in `queries/` and are re-exported from `queries/mod.rs`.
They derive `serde::Serialize` so the MCP layer serializes them directly.

## Testing

- `duration_in_value_range`: rows in/out of value range; in/out of window;
  `effective_end IS NULL` contributes zero; `Bucket::None` total vs.
  `Bucket::Day` grouping; coding-system discrimination (LOINC vs. AASM rows
  with the same `value_quantity` do not cross).
- `longest_run` (pure): single run; two runs split by a gap > tolerance;
  gap within tolerance joins; empty input → 0; single interval.
- `longest_continuous_in_value_range`: per-day bucketing; gap_seconds
  behavior end-to-end against a seeded pool.
- Oura nightly: `long_sleep` + `Some(total)` → one `93832-4` observation
  with correct minutes/unit/times; `null` total → absent; non-`long_sleep`
  → absent; `samples_count` unchanged (still epoch count).
- MCP: both new tools constructed directly in `#[tokio::test]`, asserting
  the returned JSON shape.

Run `just check` (fmt, clippy `-D warnings`, typecheck, test, deny,
machete, `sqlx prepare --check`) before declaring complete.

## Out of scope (tracked elsewhere)

Deduped current problems/medications with real status; structured
`sync_source` failure reasons; `list_metrics`; multi-coding history with
optional bounds; `list_documents`; WASO (`103215-0`) indexing.
