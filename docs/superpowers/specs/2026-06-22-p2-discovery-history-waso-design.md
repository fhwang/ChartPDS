# P2 — metric discovery, coding self-description, multi-coding history, and WASO

*Design doc. 2026-06-22.*

## Context

A set of ergonomics/discovery requests from external MCP clients. An
agentic client knows standard vocabularies (LOINC) but cannot know which
codings a given ChartPDS store actually holds, cannot interpret codings
ChartPDS *mints* itself, must read history one code at a time through a
required window, and lacks a fragmentation signal for sleep.

This batch addresses four of those, all behind a **generic, coding-agnostic
tool surface**. No domain-specific tool is added; domain specificity stays in
the adapter (data emission) and in a self-description catalog.

Two items from the original P2 list are intentionally **not** built:

- **Document-level listing** (`list_documents`) — dropped. The stated need was
  "detect a stale or empty archive up front." Raw per-document enumeration is
  the wrong altitude: it leaks ingestion internals (content hashes, filenames),
  pushes freshness logic onto the client, and grows unbounded. The need is met
  instead by the enriched `observation_counts` (per-coding `last_effective_start`
  gives clinical recency; an empty result means an empty store) plus
  `list_notifications` for pipeline health.

## Items

### #6 — Enrich `observation_counts` (discovery)

**Problem.** `observation_counts` returns `{coding_code, count}` grouped by
`coding_code` alone. It omits `coding_system` (now that non-LOINC codings
exist, the code alone is ambiguous *and* insufficient to feed back into the
`{system, code}`-typed aggregator/history tools) and gives no sense of how much
data or over what span a coding covers.

**Change.** Grow the existing tool in place — no new tool, no parameter. Group
by `(coding_system, coding_code)` and add a date span. A human-readable display
name is deliberately **not** added: standard codings are self-describing to a
client that knows the vocabulary, and a name does not help for minted codings
(see #6b).

Query `queries/counts_per_code.rs` — struct grows (consider renaming
`CodeCount` → `MetricSummary`, as it is no longer just a count):

```rust
pub struct CodeCount {
    pub coding_system: String,
    pub coding_code: String,
    pub count: i64,
    pub first_effective_start: OffsetDateTime,
    pub last_effective_start: OffsetDateTime,
}
```

```sql
SELECT coding_system,
       coding_code AS "coding_code!: String",
       COUNT(*)    AS "count!: i64",
       MIN(effective_start) AS "first_effective_start!: OffsetDateTime",
       MAX(effective_start) AS "last_effective_start!: OffsetDateTime"
FROM observations
GROUP BY coding_system, coding_code
ORDER BY coding_system, coding_code
```

Tool output becomes
`[{coding_system, coding_code, count, first_effective_start, last_effective_start}]`.

**Notes.**
- `MIN`/`MAX` on `effective_start` is lexical (the column is `TEXT`/RFC 3339) —
  the same assumption `in_range`'s `ORDER BY effective_start` already relies on,
  so no new correctness risk.
- Additive JSON (new keys); the only break is our own exact-match test, updated.
- Requires `just prepare-sql` (SQL changed).

### #6b — `describe_codings` (self-description of minted codings)

**Problem.** ChartPDS mints at least one non-standard coding
(`aasm-sleep-stage`). A client cannot guess its system URI, its code, or — the
part that actually bites — its **value semantics**: that `value_quantity` is an
AASM stage discriminant (`0 = wake, 1 = N1, 2 = N2, 3 = N3, 4 = REM`), that
`value_string` is the matching stage name, and that "any asleep" is the range
`{min: 1, max: 4}`. This is precisely the knowledge the aggregator tools need
and a client cannot otherwise obtain.

**Change.** Add a new tool `describe_codings` (no args) returning a **static
catalog of ChartPDS-minted codings only**. LOINC and other standard codings are
excluded by design — they are self-describing, and re-documenting them invites
staleness.

- **Static, not in the DB.** Definitional reference data known at compile time:
  a small `coding_catalog` module in `chartpds-core`. No table, no migration. It
  returns all minted definitions regardless of whether data is present —
  orthogonal to `observation_counts` ("what's in the store") vs the catalog
  ("what does this minted coding mean").
- **Derived from the canonical enum.** The sleep-stage entry is built by
  iterating `AasmSleepStage` (`as_str()` + discriminant), so the catalog cannot
  drift from the encoder.

Output shape:

```json
[{
  "coding_system": "https://chartpds.fhwang.net/coding/aasm/sleep-stage",
  "coding_code": "aasm-sleep-stage",
  "description": "Per-epoch (5-minute) AASM sleep stage. One observation per 5-min epoch; effective_start/end bound the epoch.",
  "value_quantity_meaning": "AASM stage discriminant. Monotonic: 0 = wake, >=1 = asleep.",
  "value_string_meaning": "Stage name matching the discriminant.",
  "values": [
    {"value_quantity": 0, "value_string": "wake", "label": "awake"},
    {"value_quantity": 1, "value_string": "n1",   "label": "light sleep (N1, transition)"},
    {"value_quantity": 2, "value_string": "n2",   "label": "light sleep (N2)"},
    {"value_quantity": 3, "value_string": "n3",   "label": "deep / slow-wave sleep (N3)"},
    {"value_quantity": 4, "value_string": "rem",  "label": "REM"}
  ],
  "hints": ["For 'asleep' totals use value_range {min: 1, max: 4}; wake is 0."]
}]
```

Client flow: `observation_counts` → sees `aasm-sleep-stage` is present →
`describe_codings` → learns `{min:1, max:4}` is asleep → drives the
duration/longest aggregators. No hardcoding.

### #7 — `get_observation_history` (multi-coding, optional bounds)

**Problem.** `observations_in_range` takes a single bare LOINC `code` and
requires both `start` and `end`. Full-history reads need a wide artificial
window per code, one code at a time, and the bare-code match is not
system-aware.

**Change.** Add `get_observation_history({ codings: [{system, code}], since?,
until? })` and **remove** `observations_in_range` — the new tool is a strict
superset (multi-coding ⊇ single, optional bounds ⊇ required window, system-aware
⊇ bare-code), and the arg contract changes fundamentally, so "enrich in place"
would be a silent breaking rewrite. Replacement is the clean end state (pre-1.0,
no compatibility guarantee).

- **System-aware** by construction (takes coding objects), which also fixes the
  latent bare-code gap.
- **Output: a flat array** of observations ordered by
  `(coding_system, coding_code, effective_start)`. Each `Observation` already
  carries its coding fields, so a flat array groups client-side trivially and
  matches the existing array shape; server-side grouping would add a nesting the
  common single-coding case must unwrap.
- Both bounds optional/open-ended: `since` only → from then on; `until` only →
  up to then; neither → full history.

Core query: add an `observation_history` function supporting a list of
`(system, code)` pairs and optional bounds; retire `in_range`. Requires
`just prepare-sql`.

### #9 — WASO (Wake After Sleep Onset)

**Problem.** No fragmentation signal. WASO (LOINC `103215-0`) is the total wake
time *between* sleep onset and final awakening — a standard sleep-quality
metric that complements total sleep time and the longest-continuous-run
aggregator. It is **not derivable** from the generic tools: summing wake epochs
via `observation_duration_in_range` would include onset latency and post-waking
time (i.e. total wake-in-bed, not WASO), because no generic aggregator has an
onset/offset concept. So it is a genuinely new signal that must be computed
where the ordered epochs are held.

**Change.** Emit one WASO observation per night from the Oura adapter. This adds
**no tool surface** — it is one more LOINC observation flowing through the
generic tools (discoverable via `observation_counts`, readable via
`get_observation_history`). It parallels the existing `nightly_sleep_duration`
emission.

Parser `oura/parser.rs`:

```rust
pub struct ParsedWaso {
    pub effective_start: OffsetDateTime,  // session bedtime_start
    pub effective_end:   OffsetDateTime,  // session bedtime_end
    pub minutes: f64,
}

pub fn wake_after_sleep_onset(session: &OuraSleepSession) -> sources::Result<Option<ParsedWaso>>
```

Algorithm:
1. Gate on `session_type == "long_sleep"` (naps still emit per-epoch stages but
   no WASO summary). Does **not** require `total_sleep_duration` — reads epochs.
2. Map `sleep_phase_5_min` chars via existing `oura_char_to_aasm`.
3. `onset` = first index whose stage ≠ `Wake`; `final_wake` = last index whose
   stage ≠ `Wake`.
4. No onset (all-wake or empty) → `Ok(None)`, emit nothing.
5. WASO = count of `Wake` epochs in the interior `[onset, final_wake]` ×
   5 min (`EPOCH_SECONDS / 60`). Leading wakes (latency) and trailing wakes
   (after final waking) fall outside `[onset, final_wake]` and are excluded.

Worked example — `sleep_phase_5_min = "4422413"` (`4` = wake): stages
`W W N2 N2 W N1 REM`, `onset = 2`, `final_wake = 6`; one interior wake (idx 4) →
**WASO = 5 min**. Leading wakes (idx 0–1) ignored.

Edge cases: unbroken night → **0 min, emitted** (a real "consolidated night"
datapoint); no sleep at all → **`None`**.

Observation, emitted in `oura/storage.rs` next to `nightly_sleep_duration` so
both ingest and rebuild/replay derive it together:

| field | value |
|---|---|
| `coding_system` | `http://loinc.org` |
| `coding_code` | `103215-0` |
| `coding_display` | `Wake after sleep onset` |
| `effective_start` / `effective_end` | session bounds (one row per night) |
| `value_quantity` | WASO minutes |
| `value_unit` | `min` |

- **No schema change, no migration, no new query** — reuses the existing
  observation-insert helper, so no `.sqlx` cache change.
- **No `describe_codings` entry** — WASO is LOINC, hence self-describing.
- **Backfill:** existing archived Oura blobs gain WASO via a `rebuild_index`
  call (storage re-derives on replay); no re-pull from Oura. The deploy step is
  "run `rebuild_index` to populate historical WASO."

## Non-goals / boundaries

- No sleep-specific *tool*. The surface stays generic; sleep specificity lives
  in the Oura adapter (emission) and the minted-coding catalog (description).
- `aasm-sleep-stage` stays as-is — it is load-bearing for the
  longest-continuous-run aggregator (inherently per-epoch).
- **Organizational watch-item (not acted on now):** `oura/parser.rs` is
  accumulating sleep derivations (epoch→stages, →total, →WASO). At three small
  pure functions a split is not warranted; if sleep metrics keep growing
  (efficiency, latency, …), group them into an `oura/sleep_metrics.rs` submodule.

## Testing

- **#6:** query tests for `(system, code)` grouping and `MIN`/`MAX` span; update
  the exact-match tool test.
- **#6b:** catalog test asserting the sleep-stage entry matches the
  `AasmSleepStage` enum (drift guard); server test for the tool.
- **#7:** query tests for multi-coding selection, each open-ended bound, and
  flat ordering; server test; remove `observations_in_range` tests.
- **#9:** parser unit tests per branch (latency-excluded, after-wake-excluded,
  fragmented count, unbroken → 0, all-wake → None, empty → None); storage test
  asserting the observation is emitted with the right code/unit; replay/rebuild
  coverage.

## Surface after this work

Tools: `ingest_record`, `latest_observation_by_code`, **`get_observation_history`**
(replaces `observations_in_range`), **`observation_counts`** (enriched),
`observation_duration_in_range`, `observation_longest_period_in_range`,
**`describe_codings`** (new), `list_problems`, `list_medications`,
`connect_source`, `sync_source`, `rebuild_index`, `list_notifications`.

Run `just check` and `just prepare-sql` before declaring complete.
