# Intra-day analysis via hour bucketing — ChartPDS implementation design

**Status:** approved, ready for implementation plan.

**Origin:** feature request `2026-07-01-intraday-hour-bucketing-design.md` in the
vitals-harness repo, driven by ChartPDS issue #4 ("Ensure intra-day analysis is
possible via MCP"). Motivating case: build a per-clock-hour histogram of
"minutes awake during sleep" across ~9 nights of Oura data to test whether
mid-night wakes cluster late-night vs. early-morning.

## Goal

Extend the existing `observation_duration_in_range` MCP tool with:

1. A new `bucket` value `"hour"` (finer than the existing `"day"`).
2. A new optional `timezone` parameter (an IANA zone name, DST-aware) that sets
   the bucket boundaries for both `day` and `hour`.

No new tool, no change to the value-range aggregation core. Default behavior is
byte-identical to today (timezone defaults to UTC), so existing callers are
unaffected.

## Background: current behavior

`observation_duration_in_range` sums the minutes a coded periodic signal spent
inside `[value_min, value_max]` over a half-open `[start, end)` window, matched
by `{coding_system, coding_code}`. Its `bucket` argument accepts:

- `"none"` (default) → `{ total_minutes }`
- `"day"` → `{ per_bucket: [{ bucket_start, total_minutes }] }`, grouped by **UTC
  day** of `effective_start`, with `bucket_start` emitted as a `"YYYY-MM-DD"`
  string.

The sum runs entirely in SQLite so high-volume signals (e.g. heart rate) never
ship rows to Rust. Intervals are attributed whole to the bucket of their
`effective_start` (a midnight-crossing interval lands entirely in the start
day's bucket).

The raw material for intra-day analysis already exists: Oura sleep data is
stored as per-epoch (5-minute) AASM sleep-stage observations. A wake epoch has
`value_quantity == 0` (AASM `Wake` discriminant; Oura's raw `'4'` char maps to
`Wake`). So `value_min:0, value_max:0` selects wake epochs. The aggregator just
can't express sub-day, local-time buckets.

## Two firm constraints (not design choices)

- **An IANA tz database is required.** The point of the request is DST-aware
  wall-clock bucketing — a folded multi-night histogram must be stable across
  the EDT/EST boundary. A fixed UTC-offset parameter cannot deliver that, so it
  is off the table.
- **Local-time bucketing cannot run in SQLite.** SQLite's `strftime` has no tz
  database. For the `hour` and `timezone` paths the bucket key must be computed
  in Rust.

## Design

### Timezone engine: `jiff`

Add [`jiff`](https://docs.rs/jiff) (0.2, MIT/Apache-2.0, bundled IANA tzdb) to
`crates/chartpds-core/Cargo.toml` — the core crate only; not the workspace root.
`jiff` is a self-contained datetime stack used *only* at the bucketing boundary:
a `time::OffsetDateTime` is converted to a `jiff` zoned instant via its Unix
timestamp, truncated to the local hour/day, and read back. The workspace's
primary datetime type stays `time::OffsetDateTime` everywhere else.

It must clear the dependency gates: `cargo deny` (license allow-list +
`multiple-versions = "deny"`) and `cargo machete`. If a duplicate-version ban
fires, prefer fixing the graph over relaxing the policy.

### Core query (`queries/duration_in_value_range.rs`)

- `Bucket` enum gains `Hour`.
- `DurationInValueRangeParams` gains `timezone: Option<&str>` (an IANA name;
  `None` = UTC).
- Two paths, split by whether local-time math is needed:
  - **Legacy SQL path** — `Bucket::None`, and `Bucket::Day` with
    `timezone == None`. Unchanged. The sum stays in SQLite; `day` still emits
    `"YYYY-MM-DD"` strings. Existing callers get byte-identical output.
  - **Local-time path** — `Bucket::Hour` (any timezone, including the UTC
    default) or any bucket with `timezone: Some`. SQL still filters coding +
    `[start, end)` window + value-range and computes each matching row's
    duration in seconds; it ships `(effective_start, duration_seconds)` rows to
    Rust. Rust converts each `effective_start` into the target zone via `jiff`,
    truncates to the local hour (or local midnight for `day`), sums durations
    per bucket key, and emits `bucket_start` as RFC 3339 with the correct local
    offset. Empty buckets are omitted; output is sorted ascending by
    `bucket_start`.
- **Attribution semantics are unchanged.** A whole interval is credited to the
  bucket of its `effective_start` — no interval-splitting. The existing
  CLAUDE.md caveat about day-bucketing continues to hold.
- **DST is free.** We only ever convert instant → local, which is always
  unambiguous; `jiff`'s tzdb yields 23- or 25-hour local days automatically, so
  folding on local hour-of-day is stable across a transition.
- **Invalid IANA name** produces a typed error, surfaced as `invalid_params` at
  the MCP layer.

### `bucket_start` output format

The format is deliberately asymmetric to preserve back-compat: only the legacy
UTC-day path keeps the bare date string; every local-time path uses RFC 3339
with the local offset.

| bucket  | timezone            | `bucket_start`                    |
| ------- | ------------------- | --------------------------------- |
| `day`   | omitted (UTC)       | `"2026-01-01"` (unchanged)        |
| `day`   | `America/New_York`  | `"2026-06-27T00:00:00-04:00"`     |
| `hour`  | omitted (UTC)       | `"2026-06-27T06:00:00Z"`          |
| `hour`  | `America/New_York`  | `"2026-06-27T02:00:00-04:00"`     |

### MCP tool (`server.rs`)

- `ObservationDurationInRangeArgs` gains `timezone: Option<String>`.
- The bucket parser accepts `"none" | "day" | "hour"`; an unknown value →
  `invalid_params`.
- `timezone: Some` combined with `bucket: "none"` is **accepted as a no-op**
  (the window is already an explicit half-open `[start, end)`), not rejected.
- The tool description is updated to document `"hour"`, the `timezone` parameter
  (default UTC), and the `bucket_start` format table.

## Worked example — the motivating query

```jsonc
observation_duration_in_range({
  coding:   { system: "https://chartpds.fhwang.net/coding/aasm/sleep-stage",
              code:   "aasm-sleep-stage" },
  start:    "2026-06-19T00:00:00-04:00",
  end:      "2026-06-28T12:00:00-04:00",
  value_min: 0, value_max: 0,          // wake epochs only
  bucket:    "hour",
  timezone:  "America/New_York"
})
// => { per_bucket: [
//      { bucket_start: "2026-06-27T00:00:00-04:00", total_minutes: 20 },
//      { bucket_start: "2026-06-27T02:00:00-04:00", total_minutes: 10 },
//      ... ] }
```

The client folds these local-hour buckets onto a 0–23 axis
(`group by hour-of-day, sum minutes`) to build the histogram. That fold is
cheap client-side work over a few hundred small rows.

## Testing

- **Core unit tests** (`duration_in_value_range.rs`): hour bucketing in UTC;
  hour + `America/New_York` producing local-hour buckets that differ from the
  UTC hour; day + timezone cutting at local midnight; a DST-spanning fold
  sanity check.
- **MCP server test** (`server.rs`): hour + timezone through the tool surface.
- **Holdout test (protected)** — a new `holdout/tests/intraday_hour_bucketing.rs`
  plus an Oura fixture under `holdout/fixtures/`. A hand-crafted Oura
  sleep-session blob (raw JSON + `.meta.json` sidecar, filename = SHA-256 of the
  bytes) places wake epochs (`'4'`) at known local hours on a night that
  **crosses UTC midnight**, so UTC-hour and local-hour bucketing land in
  visibly different buckets. The test does `rebuild_index`, then calls
  `observation_duration_in_range` with `bucket:"hour"`,
  `timezone:"America/New_York"`, `value_min:0, value_max:0`, and asserts the
  wake-minutes fall in the correct **local**-hour `bucket_start`s (carrying
  `-04:00`); as the anti-regression contrast, the same query without `timezone`
  (UTC) puts them in different buckets. Per holdout rules the test is left
  **staged-but-uncommitted** and handed off for a human `just holdout-bless`.

## Explicitly out of scope

- Server-side hour-of-day fold / histogram tool (the fold is cheap client-side;
  keeping `bucket` a pure time-series axis is the cleaner API).
- WASO / sleep-onset bracketing (`value_min:0, value_max:0` counts all wake
  epochs, including pre-onset latency and post-final-wake time).
- Other fold axes (`day-of-week`, `hour-of-week`).
- Any change to `observation_longest_period_in_range`.

## Definition of done

`just check` is green (fmt, lint with `-D warnings`, typecheck, test including
holdout, `cargo deny`, `cargo machete`, `cargo sqlx prepare --check`). The
holdout test is staged and reproduces the local-time behavior; the human blesses
it.
