# frontier_stuck Notification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `frontier_stuck:{adapter}` warning notification that fires when a source's freshness frontier has not advanced for 48h despite the adapter being otherwise healthy.

**Architecture:** Add a third pure condition to the notification evaluator (`evaluator.rs`), thread a `now` timestamp through `evaluate_all`/`evaluate_and_dispatch`, and feed the already-persisted `source_state.frontier_last_advanced_at` into the evaluator input from `sync::tick::adapter_condition`. No schema or SQL-cache change — the column and its `get_source_state` read already exist (migration `0012`, PR #17).

**Tech Stack:** Rust, `time` crate (`OffsetDateTime`, RFC3339), `sqlx` (unchanged here), `just` task runner.

## Global Constraints

- Never bypass a lint. `just check` runs clippy with `-D warnings`; every `pub` item needs a doc comment. No `#[allow(...)]` without `reason = "..."`.
- Condition id format: `frontier_stuck:{adapter_name}` (matches existing `auth_expired:{}` / `sync_failures:{}`).
- Severity: `"warning"`.
- Threshold: `FRONTIER_STUCK_THRESHOLD_HOURS = 48`, compared with `>=`.
- Refire cadence: unchanged global 24h in `dispatch.rs`. Do NOT add per-condition cadence machinery.
- "Otherwise healthy" gate: `!auth_failed && consecutive_failures == 0`.
- `frontier_last_advanced_at` absent/unparseable ⇒ `None` ⇒ never fires.
- Title copy: `ChartPDS: {display_name} data has stopped updating`
- Message copy: `The {display_name} adapter is syncing successfully but has received no new data for over {N} hours. This usually means the upstream source stopped uploading — e.g. its companion app hasn't been opened. Check the device or app.` (`{N}` = the threshold constant.)
- Do NOT touch anything under `holdout/`. This feature is core-unit-tested only (not holdout-testable).
- Run `just check` before declaring the work complete.

---

### Task 1: `frontier_stuck` condition in the evaluator + dispatcher threads `now`

Adds the pure condition and its unit tests, changes `evaluate_all` to accept `now`, and updates the single production caller plus all construction sites so the crate compiles. In this task, `sync/tick.rs` sets the new field to `None` as a placeholder; Task 2 wires the real value.

**Files:**
- Modify: `crates/chartpds-core/src/notifications/evaluator.rs`
- Modify: `crates/chartpds-core/src/notifications/dispatch.rs`
- Modify: `crates/chartpds-core/src/sync/tick.rs` (placeholder field only)
- Test: unit tests inside `crates/chartpds-core/src/notifications/evaluator.rs` (`mod tests`)

**Interfaces:**
- Consumes: `index::get_source_state` return shape is unchanged; nothing from other tasks.
- Produces:
  - `AdapterConditionState` gains field `pub frontier_last_advanced_at: Option<time::OffsetDateTime>`.
  - `evaluate_all(input: &ConditionsInput, now: OffsetDateTime) -> Vec<ConditionEvaluation>` (new second parameter). Task 2 does not call this directly, but relies on the new struct field existing.

- [ ] **Step 1: Write the failing tests**

In `crates/chartpds-core/src/notifications/evaluator.rs`, update the `mod tests` block. First add an import and a timestamp helper at the top of `mod tests` (just under `use super::*;`):

```rust
    use time::format_description::well_known::Rfc3339;

    fn ts(s: &str) -> OffsetDateTime {
        OffsetDateTime::parse(s, &Rfc3339).expect("valid timestamp")
    }
```

Update the existing `make_adapter` helper to set the new field:

```rust
    fn make_adapter(auth_failed: bool, consecutive_failures: i64) -> AdapterConditionState {
        AdapterConditionState {
            adapter_name: "fitbit".to_owned(),
            display_name: "Fitbit".to_owned(),
            auth_failed,
            consecutive_failures,
            frontier_last_advanced_at: None,
        }
    }
```

Add these new tests to `mod tests`:

```rust
    #[test]
    fn frontier_stale_and_healthy_fires_warning() {
        let mut adapter = make_adapter(false, 0);
        adapter.frontier_last_advanced_at = Some(ts("2026-01-01T00:00:00Z"));
        let now = ts("2026-01-03T01:00:00Z"); // 49h later
        let eval = eval_frontier_stuck(&adapter, now);
        assert!(eval.is_firing);
        assert_eq!(eval.condition_id, "frontier_stuck:fitbit");
        let notif = eval.notification.as_ref().expect("notification present");
        assert_eq!(notif.severity, "warning");
        assert!(notif.title.contains("stopped updating"));
        assert!(notif.title.contains("Fitbit"));
    }

    #[test]
    fn frontier_exactly_at_threshold_fires() {
        let mut adapter = make_adapter(false, 0);
        adapter.frontier_last_advanced_at = Some(ts("2026-01-01T00:00:00Z"));
        let now = ts("2026-01-03T00:00:00Z"); // exactly 48h
        assert!(eval_frontier_stuck(&adapter, now).is_firing);
    }

    #[test]
    fn frontier_recent_does_not_fire() {
        let mut adapter = make_adapter(false, 0);
        adapter.frontier_last_advanced_at = Some(ts("2026-01-01T00:00:00Z"));
        let now = ts("2026-01-02T00:00:00Z"); // 24h, under threshold
        assert!(!eval_frontier_stuck(&adapter, now).is_firing);
    }

    #[test]
    fn frontier_never_advanced_does_not_fire() {
        let adapter = make_adapter(false, 0); // frontier_last_advanced_at = None
        let now = ts("2026-06-01T00:00:00Z");
        assert!(!eval_frontier_stuck(&adapter, now).is_firing);
    }

    #[test]
    fn frontier_stale_but_auth_failed_does_not_fire() {
        let mut adapter = make_adapter(true, 0);
        adapter.frontier_last_advanced_at = Some(ts("2026-01-01T00:00:00Z"));
        let now = ts("2026-02-01T00:00:00Z");
        assert!(!eval_frontier_stuck(&adapter, now).is_firing);
    }

    #[test]
    fn frontier_stale_but_failing_does_not_fire() {
        let mut adapter = make_adapter(false, 3);
        adapter.frontier_last_advanced_at = Some(ts("2026-01-01T00:00:00Z"));
        let now = ts("2026-02-01T00:00:00Z");
        assert!(!eval_frontier_stuck(&adapter, now).is_firing);
    }
```

Replace the existing `evaluate_all_returns_two_conditions_per_adapter` test with the three-per-adapter version:

```rust
    #[test]
    fn evaluate_all_returns_three_conditions_per_adapter() {
        let input = ConditionsInput {
            adapters: vec![
                AdapterConditionState {
                    adapter_name: "fitbit".to_owned(),
                    display_name: "Fitbit".to_owned(),
                    auth_failed: true,
                    consecutive_failures: 5,
                    frontier_last_advanced_at: None,
                },
                AdapterConditionState {
                    adapter_name: "oura".to_owned(),
                    display_name: "Oura".to_owned(),
                    auth_failed: false,
                    consecutive_failures: 0,
                    frontier_last_advanced_at: None,
                },
            ],
        };
        let now = ts("2026-01-15T10:00:00Z");
        let evals = evaluate_all(&input, now);
        assert_eq!(evals.len(), 6);

        // Fitbit: auth + sync fire; frontier does not (None frontier).
        assert!(evals[0].is_firing); // auth_expired:fitbit
        assert!(evals[1].is_firing); // sync_failures:fitbit
        assert!(!evals[2].is_firing); // frontier_stuck:fitbit
        assert_eq!(evals[2].condition_id, "frontier_stuck:fitbit");

        // Oura: none fire.
        assert!(!evals[3].is_firing); // auth_expired:oura
        assert!(!evals[4].is_firing); // sync_failures:oura
        assert!(!evals[5].is_firing); // frontier_stuck:oura
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p chartpds-core --lib notifications::evaluator 2>&1 | tail -20`
Expected: FAIL — compile errors (`eval_frontier_stuck` not found, missing struct field `frontier_last_advanced_at`, `evaluate_all` takes 1 arg not 2). This confirms the tests exercise the new API.

- [ ] **Step 3: Add the struct field and the `OffsetDateTime` import**

At the top of `crates/chartpds-core/src/notifications/evaluator.rs`, add the import under the module doc comment:

```rust
use time::OffsetDateTime;
```

Add the field to `AdapterConditionState` (after `consecutive_failures`):

```rust
    /// Number of consecutive sync failures for this adapter.
    pub consecutive_failures: i64,
    /// Wall-clock time the freshness frontier last advanced, if it ever has.
    ///
    /// `None` when the source has never produced data or the stored timestamp
    /// could not be parsed; either way the `frontier_stuck` condition will not
    /// fire.
    pub frontier_last_advanced_at: Option<OffsetDateTime>,
```

- [ ] **Step 4: Add the threshold constant and `eval_frontier_stuck`**

Below the existing `SYNC_FAILURES_THRESHOLD` constant, add:

```rust
/// Hours the freshness frontier may stay unadvanced before `frontier_stuck` fires.
const FRONTIER_STUCK_THRESHOLD_HOURS: i64 = 48;
```

After `eval_sync_failures`, add:

```rust
/// Evaluate the frontier-stuck condition for a single adapter.
///
/// Fires (`warning`) only when the adapter is otherwise healthy (no auth
/// failure, no consecutive sync failures), its frontier has advanced at least
/// once, and it has not advanced for at least [`FRONTIER_STUCK_THRESHOLD_HOURS`].
/// This isolates the "syncs succeed but no new data arrives" case from the
/// failure-driven `auth_expired` / `sync_failures` conditions.
fn eval_frontier_stuck(
    adapter: &AdapterConditionState,
    now: OffsetDateTime,
) -> ConditionEvaluation {
    let condition_id = format!("frontier_stuck:{}", adapter.adapter_name);

    let healthy = !adapter.auth_failed && adapter.consecutive_failures == 0;
    let stale = adapter.frontier_last_advanced_at.is_some_and(|advanced| {
        (now - advanced).whole_hours() >= FRONTIER_STUCK_THRESHOLD_HOURS
    });

    if healthy && stale {
        ConditionEvaluation {
            condition_id: condition_id.clone(),
            is_firing: true,
            notification: Some(Notification {
                condition_id,
                severity: "warning".to_owned(),
                title: format!(
                    "ChartPDS: {} data has stopped updating",
                    adapter.display_name
                ),
                message: format!(
                    "The {} adapter is syncing successfully but has received no new \
                     data for over {} hours. This usually means the upstream source \
                     stopped uploading — e.g. its companion app hasn't been opened. \
                     Check the device or app.",
                    adapter.display_name, FRONTIER_STUCK_THRESHOLD_HOURS
                ),
            }),
        }
    } else {
        ConditionEvaluation {
            condition_id,
            is_firing: false,
            notification: None,
        }
    }
}
```

- [ ] **Step 5: Thread `now` through `evaluate_all` and push the condition**

Change `evaluate_all` to take `now` and emit the third condition:

```rust
/// Evaluate all conditions for the given input.
///
/// `now` is the wall-clock instant used for time-based conditions
/// (`frontier_stuck`). Returns one [`ConditionEvaluation`] per condition per
/// adapter.
#[must_use]
pub fn evaluate_all(input: &ConditionsInput, now: OffsetDateTime) -> Vec<ConditionEvaluation> {
    let mut out = Vec::new();
    for adapter in &input.adapters {
        out.push(eval_auth_expired(adapter));
        out.push(eval_sync_failures(adapter));
        out.push(eval_frontier_stuck(adapter, now));
    }
    out
}
```

- [ ] **Step 6: Update the dispatcher call site**

In `crates/chartpds-core/src/notifications/dispatch.rs`, `evaluate_and_dispatch` already has `OffsetDateTime` in scope. Read the clock once and pass it in:

```rust
pub async fn evaluate_and_dispatch(pool: &SqlitePool, input: &ConditionsInput) {
    let now = OffsetDateTime::now_utc();
    for eval in evaluate_all(input, now) {
        if let Err(err) = maybe_fire(pool, &eval).await {
            tracing::warn!(
                %err,
                condition = %eval.condition_id,
                "notification dispatch error"
            );
        }
    }
}
```

Also add the new field to the two test input builders in `dispatch.rs`'s `mod tests` (`firing_input` and `resolved_input`) — each constructs one `AdapterConditionState`; add `frontier_last_advanced_at: None,` after `consecutive_failures: 0,` in both.

- [ ] **Step 7: Add the placeholder field in `sync/tick.rs`**

In `crates/chartpds-core/src/sync/tick.rs`, `adapter_condition` constructs `AdapterConditionState`. Add the field as a temporary placeholder (Task 2 replaces it):

```rust
        consecutive_failures: state.as_ref().map_or(0, |s| s.consecutive_sync_failures),
        frontier_last_advanced_at: None,
    }
```

- [ ] **Step 8: Run the evaluator tests to verify they pass**

Run: `cargo test -p chartpds-core --lib notifications 2>&1 | tail -20`
Expected: PASS — all evaluator and dispatch tests green.

- [ ] **Step 9: Build the whole crate to confirm it compiles**

Run: `cargo build -p chartpds-core 2>&1 | tail -5`
Expected: `Finished` with no errors (tick.rs compiles with the placeholder field).

- [ ] **Step 10: Commit**

```bash
git add crates/chartpds-core/src/notifications/evaluator.rs \
        crates/chartpds-core/src/notifications/dispatch.rs \
        crates/chartpds-core/src/sync/tick.rs
git commit -m "Add frontier_stuck notification condition (#16)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Wire the persisted `frontier_last_advanced_at` into the tick

Replaces the Task 1 placeholder with the real value parsed from `source_state`, via a small pure helper that is directly unit-testable (valid / absent / unparseable).

**Files:**
- Modify: `crates/chartpds-core/src/sync/tick.rs`
- Test: unit test inside `crates/chartpds-core/src/sync/tick.rs` (`mod tests`)

**Interfaces:**
- Consumes: `AdapterConditionState.frontier_last_advanced_at: Option<OffsetDateTime>` (from Task 1); `index::SourceState.frontier_last_advanced_at: Option<String>` (existing).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Write the failing test**

In `crates/chartpds-core/src/sync/tick.rs`, add to `mod tests`:

```rust
    #[test]
    fn parse_frontier_advanced_handles_valid_absent_and_invalid() {
        assert!(super::parse_frontier_advanced(Some("2026-01-15T10:00:00Z")).is_some());
        assert!(super::parse_frontier_advanced(None).is_none());
        assert!(super::parse_frontier_advanced(Some("not-a-timestamp")).is_none());
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p chartpds-core --lib sync::tick::tests::parse_frontier_advanced 2>&1 | tail -20`
Expected: FAIL — compile error, `parse_frontier_advanced` not found in module `sync::tick`.

- [ ] **Step 3: Add the pure helper and use it in `adapter_condition`**

In `crates/chartpds-core/src/sync/tick.rs`, add the helper near `adapter_condition` (use fully-qualified `time::` paths to match the existing `now_rfc3339` style):

```rust
/// Parse a stored RFC3339 `frontier_last_advanced_at` string into an
/// `OffsetDateTime`.
///
/// Returns `None` when absent or unparseable, so the `frontier_stuck` condition
/// never fires on a source with no valid frontier timestamp.
fn parse_frontier_advanced(raw: Option<&str>) -> Option<time::OffsetDateTime> {
    raw.and_then(|s| {
        time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).ok()
    })
}
```

Replace the placeholder line from Task 1 in `adapter_condition`:

```rust
        consecutive_failures: state.as_ref().map_or(0, |s| s.consecutive_sync_failures),
        frontier_last_advanced_at: parse_frontier_advanced(
            state.as_ref().and_then(|s| s.frontier_last_advanced_at.as_deref()),
        ),
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p chartpds-core --lib sync::tick::tests::parse_frontier_advanced 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/sync/tick.rs
git commit -m "Feed persisted frontier timestamp into frontier_stuck condition (#16)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Full verification gate

Runs the project's full check suite to confirm fmt, clippy (`-D warnings`, incl. `missing_docs`), tests, `cargo deny`, `cargo machete`, and the sqlx offline-cache check all pass. No code changes expected; this task exists as its own reviewer gate.

**Files:** none (verification only).

- [ ] **Step 1: Run the full check suite**

Run: `just check 2>&1 | tail -40`
Expected: all stages pass. In particular: no clippy warnings (every new `pub` item — the struct field — has a doc comment), `cargo sqlx prepare --check` passes unchanged (no query touched), and `holdout-verify` passes (no protected file touched).

- [ ] **Step 2: If anything fails, fix it in `crates/**` and re-run**

Do NOT edit anything under `holdout/`. If `holdout-verify` fails, you modified a protected file — revert that change. If clippy flags a missing doc, add the doc comment. Re-run `just check` until green.

- [ ] **Step 3: Confirmation**

Confirm the branch is clean and all three commits are present:

Run: `git log --oneline -4 && git status --short`
Expected: the design commit plus Task 1 and Task 2 commits; clean working tree.

## Notes for the implementer

- The `frontier_last_advanced_at` column and its `get_source_state` read already exist (migration `0012`, PR #17). Do **not** add a migration or run `just prepare-sql` — no SQL query changes in this plan.
- `Option<OffsetDateTime>` is `Copy`, so `adapter.frontier_last_advanced_at.is_some_and(...)` in Task 1 does not move out of the borrow.
- Keep the global 24h refire cadence in `dispatch.rs` untouched. The design deliberately does **not** add per-condition cadence.
