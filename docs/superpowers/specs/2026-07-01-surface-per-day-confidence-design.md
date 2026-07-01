# Surface per-day confidence (confirmed/provisional) in query and tool responses

Issue: #15

## Problem

ChartPDS computes per-day data confidence (`Confirmed` | `Provisional`) but
uses it only internally, to decide which days sync should re-fetch. It is never
surfaced in any query or tool response. A consumer (e.g. a scheduled report)
querying recent data therefore has no way to know a day is still incomplete and
will silently treat a `provisional` day's partial value as settled fact. This is
the core "honest uncertainty" gap.

The confidence primitives already exist and are pure:

- `DayConfidence { Confirmed, Provisional }` — `sources/confidence.rs`
- `fitbit_day_confidence(today, date, freshness_frontier, day_state)` —
  `sources/fitbit/confidence.rs` (stability-based)
- `oura_day_confidence(now, date)` — `sources/oura/confidence.rs` (time-based)

What is missing is (a) a resolver that goes from the index to a confidence value
and (b) the plumbing that attaches that value to day-oriented tool responses.

## Dependency status

- **#13 (frontier persistence)** — done, merged.
- **#14 (rebuild reconstructs the frontier)** — closed WONTFIX ("the frontier and
  stability self-heal after a cold rebuild"). Consequence: a rebuilt index has a
  null frontier until live syncs run, so a black-box holdout test can never
  observe `confirmed`. This does not change the implementation (the resolver
  surfaces whichever value applies; the confirmed path lights up in production
  once syncs establish the frontier). It only constrains how we test the
  confirmed path — see Testing.

## Scope

Surface confidence on four day-oriented tool surfaces:

- `get_observation_history` — per-observation confidence.
- `latest_observation_by_code` — per-observation confidence (added beyond the
  three named in #15; it returns the single most-recent observation, i.e. the
  highest-risk consumer of silent-undercount).
- `observation_duration_in_range` — per-bucket confidence, **Buckets variant
  only**.
- `observation_longest_period_in_range` — per-bucket confidence.

Out of scope: the `duration` **Total** variant (`Bucket::None`) — one flag over a
whole multi-day window is meaningless; `list_problems` / `list_medications` (not
day-oriented); reopening or implementing #14; any frontier-reconstruction work.

The change is additive at the JSON level: existing fields keep their names and
positions, so no consumer breaks.

## Architecture

### Decoration boundary: fold into the core query functions

Confidence is attached **inside the four core query functions**, not in a
separate MCP-layer decorator. Rationale:

- The bucket queries (`duration`, `longest`) do row selection (coding + window +
  value range) inside SQL. Decorating at the MCP layer would require re-expressing
  that same `WHERE` clause in a second place to learn which observations feed each
  bucket; the two filters would drift. Folding keeps each tool's selection filter
  in exactly one place.
- It is what #15 asks for ("surface confidence in the query responses").
- The only callers of these four functions are the MCP server and their own unit
  tests, so the return-type churn is contained.

Each query function gains a `now: OffsetDateTime` parameter. It is injected (not
read from an internal clock) so tests are deterministic. The MCP server passes
`OffsetDateTime::now_utc()`.

### The resolver — `queries/day_confidence.rs`

A new core query primitive:

```rust
pub async fn resolve_source_day_confidence(
    pool: &SqlitePool,
    now: OffsetDateTime,
    keys: &[(String, String)],           // (source, date) pairs, YYYY-MM-DD
) -> Result<HashMap<(String, String), DayConfidence>, sqlx::Error>
```

Behavior:

1. Group the requested keys by source.
2. For each source that needs it, fetch `source_state.freshness_frontier_at`
   once (via `get_source_state`).
3. Look up `source_day_state` for each `(source, date)` key (batched; small
   cardinality).
4. Dispatch per source:
   - `"fitbit"` → `fitbit_day_confidence(today, date, frontier, day_state)`
     where `today` is `now`'s UTC calendar date formatted `YYYY-MM-DD`.
   - `"oura"` → `oura_day_confidence(now, date)`.
   - any other source → `Confirmed` (policy; see Edge cases).

The resolver is the single place that reads `source_state` / `source_day_state`
and calls the pure per-adapter functions. It has no wall clock of its own; `now`
is always passed in.

### Observation surfaces (history + latest)

New serialization wrapper:

```rust
#[derive(serde::Serialize)]
pub struct ObservationWithConfidence {
    #[serde(flatten)]
    pub observation: Observation,
    pub confidence: DayConfidence,
}
```

`#[serde(flatten)]` produces the additive flat JSON — every existing
`Observation` field stays put and `confidence` joins as a sibling key. The
`Observation` struct itself is NOT modified (it is shared across the codebase).

Decoration steps inside `observation_history` (and analogously
`latest_by_code`):

1. Run the existing query to get the `Observation`(s).
2. Collect distinct `source_document_id`s.
3. Look up each document's `(source, document_date)` from `source_documents`.
4. Build resolver keys only for documents with `document_date = Some(date)` and a
   wearable source; resolve them.
5. Map each observation to its confidence:
   - `document_date = None` → `Confirmed`.
   - non-wearable source → `Confirmed`.
   - otherwise → the resolved value.
6. Return `Vec<ObservationWithConfidence>` (or a single one for `latest`).

### Bucket surfaces (duration Buckets + longest)

Add a field to each bucket type:

```rust
pub struct BucketMinutes { /* existing */  pub confidence: DayConfidence }
pub struct BucketLongest { /* existing */  pub confidence: DayConfidence }
```

After the aggregation query runs, a **companion query over the same filter**
returns, per UTC bucket, the distinct contributing `(source, document_date)`
pairs:

```sql
SELECT date(o.effective_start) AS bucket, sd.source, sd.document_date
FROM observations o
JOIN source_documents sd ON o.source_document_id = sd.id
WHERE <same selection filter as the aggregation>
GROUP BY bucket, sd.source, sd.document_date
```

Each `(source, document_date)` is resolved (with `None` date / non-wearable →
`Confirmed`), then rolled up per bucket:

> **Roll-up rule:** a bucket is `Provisional` if ANY contributing source-day is
> `Provisional`; otherwise `Confirmed`.

The companion query lives in the same query module as the aggregation, so the
`WHERE` clause is authored once and shared by construction (extracted into a
shared fragment or a helper that both code paths call).

## Edge cases and policy

- **CCDA / any non-wearable source → `Confirmed`.** These have no
  `source_day_state` row and a caller-supplied source string (e.g. `"epic"`),
  never `fitbit`/`oura`. A finalized clinical document does not accrete data.
- **`document_date = None` → `Confirmed`.** No replay-day to key on.
- **Wearable day with observations but no `source_day_state` → `Provisional`.**
  Falls out of `fitbit_day_confidence` naturally (missing `day_state`).
- **Longest-run midnight-crossing subtlety.** `longest_continuous_in_value_range`
  attributes a whole run to the UTC day its first interval started, but the
  confidence roll-up keys on each contributing observation's `effective_start`
  UTC day. For a midnight-crossing run these can differ: usually this over-flags
  a bucket `provisional` based on a neighboring source-day (conservative), but a
  run whose pre-midnight samples come from a confirmed source-document and whose
  post-midnight samples come from a distinct provisional document can leave the
  run's start-day bucket reading `confirmed` despite containing provisional
  data — a narrow under-flag. `duration_in_value_range` does not have this
  issue: it buckets each interval by its own day, matching the roll-up
  exactly. Document both behaviors in the function.

## Serialization detail

Add `#[serde(rename_all = "lowercase")]` to `DayConfidence` so it serializes as
`"confirmed"` / `"provisional"`. Safe because `DayConfidence` is serialized in no
tool response today.

## Testing

Chosen strategy: core unit tests + a core integration test + a provisional-only
holdout test.

- **Core unit tests** on `resolve_source_day_confidence` (synthetic frontier +
  `source_day_state`, no rebuild): fitbit confirmed, fitbit provisional, oura
  confirmed, oura provisional, CCDA/non-wearable confirmed, `None`-date
  confirmed, and bucket roll-up (mixed contributors → `provisional` wins;
  all-confirmed → `confirmed`).
- **Core integration test** (chartpds-core, real SQLite): seed `observations` +
  `source_documents` + a `source_state` frontier row *directly* (bypassing
  `rebuild_index`, which #14 will not populate), then drive the query functions
  and assert the confirmed path end-to-end below the MCP layer.
- **Holdout — provisional case only** (feasible today): plant a recent Fitbit
  fixture under `holdout/fixtures/`, `rebuild_index`, query, and assert the
  response carries `confidence: "provisional"`. This encodes the actual
  regression: recent incomplete data must not be reported as settled. Because it
  is a NEW holdout test, it will be written staged-but-uncommitted and handed off
  for a human `just holdout-bless` — it will not be blessed by the implementer.
- The confirmed holdout case from the original #15 plan is dropped: it required
  #14's frontier reconstruction, which is WONTFIX.

## Definition of done

- `resolve_source_day_confidence` exists in `queries/` with `mod` + `pub use`
  wiring and cached SQL (`just prepare-sql`).
- All four tool surfaces emit a lowercase `confidence` field in the shape above;
  existing fields unchanged.
- Core unit tests + core integration test pass.
- A provisional-case holdout test is written and staged for blessing (not
  blessed by the implementer).
- `just check` passes (fmt, lint, typecheck, test, deny, machete, sqlx prepare
  check, holdout-verify).
