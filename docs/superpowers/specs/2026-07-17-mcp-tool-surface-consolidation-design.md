# MCP tool surface consolidation (issue #29)

**Date:** 2026-07-17
**Status:** Approved
**Issue:** [#29 — Reassess tool surface for consistency, general API best practices](https://github.com/fhwang/ChartPDS/issues/29)

## Context

The MCP tool surface grew piecemeal to 18 tools. A full survey found the drift
you'd expect: mixed naming grammars, two time-window vocabularies, five
different bucket vocabularies across the aggregate family, an overloaded
`gap_seconds` parameter with two meanings, four response-envelope shapes, and a
handful of small bugs (missing `timezone` on one tool, missing `gap_seconds`
validation on another, stale tool descriptions, misclassified errors).

The repo is alpha; breaking the public interface is acceptable. This spec
defines a one-time breaking cleanup: consolidate near-duplicate tools and
normalize every name, parameter, and response shape. **No back-compat
aliases.**

## 1. Tool catalog — 18 → 16, noun-first naming

Naming grammar is noun-first (`resource_verb` / `resource_qualifier`),
following the AWS domain-noun-verb MCP guidance: related tools sort and read
as families, which helps an LLM client build a mental map of the catalog.

| Current | New |
|---|---|
| `ingest_record` | `record_ingest` |
| `latest_observation_by_code` | `observation_latest` |
| `get_observation_history` | `observation_history` |
| `observation_counts` | `observation_codings` |
| `describe_codings` | `coding_definitions` |
| `observation_stats` | `observation_stats` (unchanged) |
| `observation_table` | `observation_table` (unchanged) |
| `observation_relationship` | `observation_relationship` (unchanged) |
| `observation_duration_in_range` | **deleted** — becomes the `duration_in_range` column aggregate |
| `observation_longest_period_in_range` | **deleted** — becomes the new `longest_run_in_range` column aggregate |
| `list_problems` | `problem_list` |
| `list_medications` | `medication_list` |
| `connect_source` | `source_connect` |
| `sync_source` | `source_sync` |
| `rebuild_index` | `index_rebuild` |
| `list_notifications` | `notification_list` |
| `search_narratives` | `narrative_search` |
| `get_narrative` | `narrative_get` |

Consolidation rationale: `observation_table` already had `duration_in_range`
as a column aggregate, making the standalone tool a single-column special
case. Both in-range tools fold into the shared column spec, usable from
`observation_table` and `observation_relationship`. `observation_latest`
stays a distinct tool (it is a deliberate convenience, not a duplicate) but
adopts the standard coding selector. `observation_stats` and
`observation_table` stay separate: stats is one coding with a full
distribution per bucket; table is many codings reduced to one number each —
merging them would produce a conditional return shape that is worse for an
LLM client than two crisp tools.

## 2. Shared vocabulary

**Coding selector.** Every observation tool takes `coding: {system, code}`.
`observation_latest` adopts it (replacing its bare LOINC-only `code` string),
which also unlocks latest-of-minted-codings (sleep stage, WASO) for the first
time.

**Time window.** `start` (inclusive) / `end` (exclusive), RFC 3339, **both
optional**, on every windowed tool (`observation_history`,
`observation_stats`, `observation_table`, `observation_relationship`). Omit
`start` for "from the beginning of the data", omit `end` for "through now".
The `since`/`until` names die.

**One bucket vocabulary:** `none | hour | day | week | month | day_of_week |
episode`, with per-tool subsets:

- `observation_stats`: all seven (default `none`). Gains `hour`.
- `observation_table`: all seven (default `day`). Gains `none` (one
  whole-window row), `hour`, and `day_of_week` — re-homing the deleted
  duration tool's buckets.
- `observation_relationship`: the sequence-able subset
  `hour | day | week | month | episode` (default `day`). `none` cannot pair;
  `day_of_week` folds weeks together so lag is meaningless. Gains `episode`,
  closing an accidental gap.

**Episode spec.** An explicit `episode: {coding: {system, code},
gap_seconds?}` object, required when (and only valid when)
`bucket: "episode"`, identical on stats/table/relationship. Episodes are
gap-tolerant chains of the spec coding's interval observations, keyed by the
episode's RFC 3339 UTC start instant. Stats loses its implicit
"episodes of the queried coding" shorthand in exchange for generality
(episodes may be defined by a different coding than the one aggregated).

**Column spec** (table columns; relationship `x`/`y`):

```
{coding: {system, code}, aggregate?, field?, value_min?, value_max?, gap_seconds?}
```

- `aggregate`: `mean` (default) | `sum` | `min` | `max` | `count` | `median`
  | `duration_in_range` | `longest_run_in_range`.
- The two range aggregates require `value_min`/`value_max` (inclusive) and
  report minutes; `longest_run_in_range` also accepts per-column
  `gap_seconds` (allowed gap between consecutive in-range intervals before a
  run breaks; default 0).
- `field`: `value` (default) | `start_time_of_day` | `end_time_of_day` |
  `interval_minutes`; ignored by the range aggregates.
- `value_min`/`value_max`/`gap_seconds` are invalid outside the aggregates
  that use them.

This ends the `gap_seconds` overload: **column-level `gap_seconds` = run
tolerance; episode-level `gap_seconds` = chaining.** They are different knobs
with different homes.

**Timezone.** Optional IANA name (default UTC) on every bucketed tool,
governing all bucket boundaries and time-of-day derivation.
`observation_longest_period_in_range`'s missing-timezone gap dies with the
tool (its replacement, the column aggregate, inherits the table's timezone).

**Validation, uniformly:** `gap_seconds >= 0` (fixing the missing check in
the longest-period path), `value_min <= value_max`, unknown enum values
rejected with an `invalid_params` error listing the accepted values.

## 3. Response shapes

- **List tools return `{items: [...]}`** — never a bare array:
  `observation_history`, `observation_codings`, `coding_definitions`,
  `problem_list`, `medication_list`, `notification_list`, `narrative_search`,
  `source_sync` (its `results` key becomes `items`). `problem_list` and
  `medication_list` keep `latest_document_date` beside `items`.
- **Bucketed stats returns `{items: [{bucket_key, ...}]}`** (replacing
  `per_bucket`); `bucket: "none"` keeps the flat whole-window stats object.
- **`observation_table` keeps `{columns, rows}`**; each row is
  `{bucket_key, values, confidence}`.
- **`bucket_key` formats, fixed per bucket type everywhere:**
  - `day` → `YYYY-MM-DD` (in the request timezone)
  - `week` → the ISO week's Monday as `YYYY-MM-DD`
  - `month` → `YYYY-MM`
  - `day_of_week` → `mon`..`sun`
  - `hour` → RFC 3339 with the local offset
  - `episode` → the episode's RFC 3339 UTC start instant
  - `none` → `null`

  The old duration tool's "date-only for day+UTC, RFC 3339 otherwise"
  wobble dies with the tool.
- **Singular tools** (`observation_latest`, `narrative_get`) return the
  resource object directly, or `null` when absent. `narrative_get`'s
  argument renames `document_id` → `source_document_id`, matching the field
  name every response uses.
- **`source_connect` returns structured JSON** like everything else:
  `{source, status: "authorization_pending" | "connected",
  authorization_url?, message}` (prose instructions move into `message`).
- **`index_rebuild`'s description** is corrected to the true `RebuildResult`
  field list (including `narratives_ingested`, `extractions_applied`).
- **Errors:** invalid arguments and FTS5 *syntax* errors → `invalid_params`;
  genuine database failures → `internal_error` (fixing `narrative_search`,
  which today collapses both into `invalid_params`).

## 4. Core changes (`chartpds-core`)

- `aligned_table`'s machinery absorbs the two deleted queries:
  `ColumnAggregate::LongestRunInRange { value_min, value_max, gap_seconds }`,
  plus `none`/`hour`/`day_of_week` buckets for the table path.
- The `duration_in_value_range` and `longest_continuous_in_value_range`
  query modules retire. Their interval-clamping and run-chaining logic (and
  known-answer tests) port into the shared aggregate path.
- `observation_stats` gains the explicit episode-spec parameter and optional
  bounds; `latest_by_code` becomes coding-based (`latest_by_coding`).
- Per-bucket confidence roll-up continues to apply everywhere it does today.
- `just prepare-sql` after query changes; CLAUDE.md's Queries and MCP server
  sections updated to the new catalog.

## 5. Migration & holdout plan

The holdout suite binds to the MCP tool surface: 41 tool-name references
across all 11 holdout test files, including 4 uses of the deleted
`observation_duration_in_range`. Holdout is read-only to the agent, so:

1. Implementation lands on a branch; all 11 holdout files fail — expected
   and legitimate, since the interface change is deliberate.
2. When everything else is green, the human explicitly asks the agent to
   draft the holdout updates: mechanical renames plus rewriting the
   duration-tool tests against `observation_table` (`hour` bucket + range
   column aggregates). The draft stays staged-uncommitted.
3. The human reviews and admits it via `just holdout-bless` (signed commit),
   then merges.

## Out of scope (follow-ups)

- `confidence` on `observation_history` / `observation_latest` rows.
- CLI subcommands mirroring the tool catalog.
- Pagination (`{items}` envelopes leave room for it later).
