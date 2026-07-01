# frontier_stuck notification — design

Issue #16. Adds a notification condition that fires when a source's freshness
frontier has not advanced for a sustained period — sync keeps succeeding but no
genuinely new data is arriving. This is the "upstream data stopped flowing,
human action may be required" nudge (e.g. a wearable whose companion app hasn't
been opened). Sync is otherwise eventually-correct on its own; this condition
covers the one failure mode it cannot fix itself.

## Why a distinct condition

The path is `device → vendor cloud → ChartPDS sync → index`. The two existing
conditions detect failures on ChartPDS's own side, where an operation *fails*:

- `auth_expired` — the API rejected us; our token is bad.
- `sync_failures` — the sync call errored 3+ times in a row.

`frontier_stuck` detects the case where **nothing fails**: syncs succeed
(`200 OK`, clean parse, `consecutive_failures == 0`), but the data returned is
the same data we already had — the freshness frontier does not advance. From
every signal the existing conditions watch, everything is green, yet new data
silently stopped arriving. `sync_failures` answers "is our fetching broken?";
`frontier_stuck` answers "is new data actually arriving?" Those come apart
exactly when syncs succeed but return nothing new.

## Dependency (satisfied)

Depends on #13, which is merged (PR #17). `source_state.frontier_last_advanced_at`
— a durable, wall-clock RFC3339 timestamp of the last time the frontier moved
forward — is persisted (migration `0012`) and already returned by
`get_source_state`. No schema or query change is needed for this work.

## Behavior

A condition `frontier_stuck:{adapter}`, severity `warning`, fires when **all** of:

1. **Adapter is otherwise healthy** — no auth failure and `consecutive_failures
   == 0`. A stale frontier on a *failing* source is a symptom already owned by
   `sync_failures`/`auth_expired`; gating on health keeps `frontier_stuck` a
   distinct "green everywhere else, but data stopped" signal rather than a
   duplicate alert for the same root cause.
2. **`frontier_last_advanced_at` is non-NULL** — the source has produced real
   data at least once. A brand-new source that has never flowed does not nag.
3. **`now - frontier_last_advanced_at >= 48h`** — the freshness threshold. A
   normal overnight gap or a day of not wearing the device will not trip it.

Note a deliberate gap this creates: the health gate requires
`consecutive_failures == 0`, while `sync_failures` only fires at `>= 3`. A
source with 1–2 sporadic recent failures and a stale frontier is unhealthy
enough to suppress `frontier_stuck` but not yet failing enough to trip
`sync_failures`, so neither condition fires for it. This is intentional:
once a source is failing at all, root-causing that failure is
`sync_failures`'s job, not `frontier_stuck`'s — the two aren't meant to be
gapless.

### Two timers, kept separate

- **Threshold (48h)** governs *when the first alert fires* — how long stale
  before we care. Issue's suggested default; chosen for this design.
- **Refire cadence** governs *how often a duplicate reminder is appended* while
  the condition stays firing. `frontier_stuck` uses the **existing global 24h**
  cadence, same as the other two conditions. No per-condition cadence machinery
  is introduced.

> The issue floated a ~7d cadence and a mechanism to make `should_refire`
> cadence-aware. We deliberately drop that: `frontier_stuck` is an actionable
> "go open your app" nudge that merits the same daily reminder as the other
> conditions, and a uniform cadence removes the need to touch `should_refire`
> or the evaluation struct. YAGNI.

## Code changes

All in `crates/chartpds-core`. Three files, no SQL/schema change.

### `notifications/evaluator.rs`

- Add field `frontier_last_advanced_at: Option<OffsetDateTime>` to
  `AdapterConditionState`.
- Add `const FRONTIER_STUCK_THRESHOLD_HOURS: i64 = 48`.
- Add `eval_frontier_stuck(adapter, now)` — the threshold comparison lives here
  (pure, unit-tested). Returns a non-firing evaluation when the frontier is
  `None`, the elapsed time is under threshold, or the adapter is unhealthy.
- Change `evaluate_all(input)` → `evaluate_all(input, now)` and push the third
  condition per adapter.

### `notifications/dispatch.rs`

- `evaluate_and_dispatch` reads `OffsetDateTime::now_utc()` once and passes it to
  `evaluate_all`. No cadence change — the global 24h `should_refire` is untouched.

### `sync/tick.rs`

- `adapter_condition` reads `state.frontier_last_advanced_at` (already selected
  by `get_source_state`), parses the RFC3339 string to `OffsetDateTime`
  (parse failure ⇒ `None` ⇒ never fires), and sets the new field.

## Message copy

- **title:** `ChartPDS: {display_name} data has stopped updating`
- **message:** `The {display_name} adapter is syncing successfully but has
  received no new data for over 48 hours. This usually means the upstream source
  stopped uploading — e.g. its companion app hasn't been opened. Check the
  device or app.`

## Testing

Core-level unit tests only. Per the issue, this is **not black-box
holdout-testable**: notification evaluation runs only inside a live
`sync::run_tick` with configured adapters and real API calls, and there is no
MCP tool to drive a tick or evaluate conditions on demand. Coverage is
core-level.

Evaluator (`eval_frontier_stuck`, with a fixed `now`):

- Fires (warning) when frontier is stale past threshold **and** adapter healthy.
- Does not fire when the frontier advanced within the threshold.
- Does not fire when `frontier_last_advanced_at` is `None`.
- Does not fire when unhealthy — `auth_failed == true`, or
  `consecutive_failures > 0` — even with a stale frontier.
- Boundary: exactly-48h behaves per the `>=` comparison.

Update the existing `evaluate_all_returns_two_conditions_per_adapter` test to
expect **three** conditions per adapter. Add the new field at all
`AdapterConditionState` construction sites (evaluator tests, `dispatch.rs`
test inputs, `sync/tick.rs`).

## Out of scope

- Per-condition refire cadence machinery (dropped by the uniform-24h decision).
- Any push delivery — notifications remain pull-only via `list_notifications`.
- A no-network tool seam to make this holdout-testable.
