# MCP Tool Surface Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the approved spec `docs/superpowers/specs/2026-07-17-mcp-tool-surface-consolidation-design.md`: consolidate the MCP surface from 18 to 16 noun-first tools with one shared bucket/episode/column vocabulary and uniform `{items}` envelopes.

**Architecture:** Core-first: extend `chartpds-core`'s shared aggregate machinery (`aligned_table` + `observation_stats`) with the buckets and aggregates the two retiring query modules provided, then rework the tool layer (`chartpds-mcp/src/server.rs`) tool-by-tool, then delete the retired modules and update docs. The holdout suite binds to the old surface and is read-only: it fails progressively from Task 6 onward, deliberately, and is redrafted by the human-blessed process after this plan completes.

**Tech Stack:** Rust (stable, pinned), sqlx offline mode, rmcp, jiff + time, serde/schemars.

## Global Constraints

- **Never modify** `holdout/`, `holdout.lock`, `.github/allowed_signers`, `.github/workflows/holdout.yml`. Holdout test failures after Task 6 are expected; do not "fix" them.
- Verification command is `env -u RUSTUP_TOOLCHAIN just check` (the env var masks the repo toolchain pin). Until Task 6, everything must pass. From Task 6 on, run `env -u RUSTUP_TOOLCHAIN cargo test --workspace --exclude holdout` plus `just lint` / `just fmt-check` / `cargo sqlx prepare --check` instead, and confirm holdout is the *only* failing crate.
- After any change to a `sqlx::query!` invocation, run `just prepare-sql` and commit the `.sqlx/` cache delta in the same commit.
- No `#[allow(...)]` without `reason = "..."`. Every `pub` item gets a doc comment (missing_docs is error-level under `just lint`).
- No back-compat aliases anywhere: old tool names, old parameter names, and old response keys must be gone at the end.
- Final tool catalog (exactly these 16 names): `record_ingest`, `observation_latest`, `observation_history`, `observation_codings`, `coding_definitions`, `observation_stats`, `observation_table`, `observation_relationship`, `problem_list`, `medication_list`, `narrative_search`, `narrative_get`, `source_connect`, `source_sync`, `notification_list`, `index_rebuild`.
- Work happens on a branch named `worktree-issue-29-tool-surface` (create via the worktree skill at execution time).

---

### Task 1: `hour` bucket in the shared stats machinery

**Files:**
- Modify: `crates/chartpds-core/src/queries/observation_stats.rs` (StatsBucket enum ~line 39, `bucket_key` ~line 365)

**Interfaces:**
- Produces: `StatsBucket::Hour`; `bucket_key(dt, StatsBucket::Hour, tz) -> Ok((0, label))` where `label` is the RFC 3339 top-of-hour instant with the local offset, e.g. `"2026-06-27T02:00:00-04:00"` (`"2026-06-27T02:00:00Z"` for UTC). Later tasks (table, relationship) rely on this exact label format.

- [ ] **Step 1: Write the failing tests** (in the existing `#[cfg(test)] mod tests` of `observation_stats.rs`)

```rust
#[test]
fn bucket_key_hour_utc_is_top_of_hour_z() {
    let (idx, label) = bucket_key(
        datetime!(2026-06-27 02:41:09 UTC),
        StatsBucket::Hour,
        &jiff::tz::TimeZone::UTC,
    )
    .expect("key");
    assert_eq!(idx, 0);
    assert_eq!(label, "2026-06-27T02:00:00Z");
}

#[test]
fn bucket_key_hour_local_carries_offset() {
    let tz = jiff::tz::TimeZone::get("America/New_York").expect("tz");
    let (_, label) = bucket_key(
        datetime!(2026-06-27 02:41:09 UTC), // 22:41 EDT on Jun 26
        StatsBucket::Hour,
        &tz,
    )
    .expect("key");
    assert_eq!(label, "2026-06-26T22:00:00-04:00");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p chartpds-core bucket_key_hour -- --nocapture`
Expected: FAIL — `Hour` is not a variant of `StatsBucket`.

- [ ] **Step 3: Implement**

Add the variant (doc comment required):

```rust
/// Per clock hour (local to the request timezone), keyed by the RFC 3339
/// top-of-hour instant with the local offset (`...Z` for UTC). Lexicographic
/// order of these keys is chronological, including across a DST fall-back
/// (the two 01:00 hours differ in offset and sort in instant order).
Hour,
```

Add the `bucket_key` arm. The existing function computes `date` from a `Zoned`; hour needs the full zoned datetime, so restructure minimally:

```rust
let zoned = to_zoned(dt, tz)?;
let date = zoned.datetime().date();
match bucket {
    // ...existing arms unchanged...
    StatsBucket::Hour => {
        let civ = zoned.datetime();
        let offset_seconds = zoned.offset().seconds();
        let (sign, abs) = if offset_seconds < 0 {
            ('-', -offset_seconds)
        } else {
            ('+', offset_seconds)
        };
        let suffix = if offset_seconds == 0 {
            "Z".to_string()
        } else {
            format!("{sign}{:02}:{:02}", abs / 3600, (abs % 3600) / 60)
        };
        Ok((
            0,
            format!(
                "{:04}-{:02}-{:02}T{:02}:00:00{suffix}",
                civ.year(),
                civ.month(),
                civ.day(),
                civ.hour()
            ),
        ))
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p chartpds-core bucket_key_hour`
Expected: PASS. Also run `cargo test -p chartpds-core observation_stats` — all green (stats itself does not accept `Hour` from the tool yet; that arrives in Task 7).

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/queries/observation_stats.rs
git commit -m "issue #29: hour bucket in shared stats bucket_key"
```

---

### Task 2: `aligned_table` gains `none` / `hour` / `day_of_week` buckets and nullable `bucket_key`

**Files:**
- Modify: `crates/chartpds-core/src/queries/aligned_table.rs`
- Modify: `crates/chartpds-core/src/queries/signal_relationship.rs` (compile fix for `bucket_key: Option<String>`)

**Interfaces:**
- Consumes: `bucket_key(...)` incl. the Task 1 `Hour` arm.
- Produces: `TableBucket::{None, Hour, Day, Week, Month, DayOfWeek, Episode}`; `TableRow.bucket_key: Option<String>` (`None` only for `TableBucket::None`; serializes as JSON `null`). Cells and row keys are internally keyed `(u8, String)` so `day_of_week` rows come out Monday-first.

- [ ] **Step 1: Write the failing tests** (in `aligned_table.rs` tests)

```rust
#[tokio::test]
async fn none_bucket_returns_one_whole_window_row_with_null_key() {
    let (pool, _) = seed_observations(&weight_and_hr_specs()).await;
    let columns = [value_column("29463-7", ColumnAggregate::Mean)];
    let mut params = day_params(&columns);
    params.bucket = TableBucket::None;
    let table = aligned_table(&pool, NOW, params).await.expect("query");
    assert_eq!(table.rows.len(), 1);
    assert_eq!(table.rows[0].bucket_key, None);
    assert_eq!(table.rows[0].values, vec![Some(400.0)]); // mean of 380/400/420
}

#[tokio::test]
async fn day_of_week_rows_are_monday_first() {
    // weight_and_hr_specs: Jan 1 2026 = Thursday, Jan 2 = Friday, Jan 3 = Saturday.
    let (pool, _) = seed_observations(&weight_and_hr_specs()).await;
    let columns = [value_column("29463-7", ColumnAggregate::Mean)];
    let mut params = day_params(&columns);
    params.bucket = TableBucket::DayOfWeek;
    let table = aligned_table(&pool, NOW, params).await.expect("query");
    let keys: Vec<_> = table.rows.iter().map(|r| r.bucket_key.clone()).collect();
    assert_eq!(
        keys,
        vec![
            Some("thu".to_string()),
            Some("fri".to_string()),
            Some("sat".to_string())
        ]
    );
}

#[tokio::test]
async fn hour_bucket_keys_by_local_hour() {
    let (pool, _) = seed_observations(&weight_and_hr_specs()).await; // 08:00 UTC each day
    let columns = [value_column("29463-7", ColumnAggregate::Count)];
    let mut params = day_params(&columns);
    params.bucket = TableBucket::Hour;
    let table = aligned_table(&pool, NOW, params).await.expect("query");
    assert_eq!(table.rows.len(), 3);
    assert_eq!(
        table.rows[0].bucket_key.as_deref(),
        Some("2026-01-01T08:00:00Z")
    );
}
```

Also update every existing assertion in this file from `rows[i].bucket_key, "..."` to `rows[i].bucket_key.as_deref(), Some("...")`.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p chartpds-core aligned_table`
Expected: FAIL — no `TableBucket::None` / `DayOfWeek` / `Hour` variants, `bucket_key` type mismatch.

- [ ] **Step 3: Implement**

1. Extend `TableBucket` (each variant doc-commented, mirroring `StatsBucket`'s wording): `None`, `Hour`, `Day`, `Week`, `Month`, `DayOfWeek`, `Episode`.
2. `TableRow.bucket_key` becomes `Option<String>` with doc: `null` only for the `none` bucket's single whole-window row.
3. `calendar_label` returns the full `(u8, String)` from `bucket_key(...)` and maps every calendar variant:

```rust
fn calendar_label(
    dt: OffsetDateTime,
    bucket: TableBucket,
    tz: &TimeZone,
) -> Result<(u8, String), AlignedTableError> {
    let stats_bucket = match bucket {
        TableBucket::None => StatsBucket::None,
        TableBucket::Hour => StatsBucket::Hour,
        TableBucket::Day => StatsBucket::Day,
        TableBucket::Week => StatsBucket::Week,
        TableBucket::Month => StatsBucket::Month,
        TableBucket::DayOfWeek => StatsBucket::DayOfWeek,
        TableBucket::Episode => {
            return Err(AlignedTableError::Internal(
                "episode buckets are not calendar buckets".to_string(),
            ))
        }
    };
    bucket_key(dt, stats_bucket, tz).map_err(map_stats_err)
}
```

4. Re-key `column_cells` return type and `contributions` grouping to `(u8, String)`; the confidence contribution label stays the plain `String` half. Row-key collection becomes `BTreeSet<(u8, String)>` (episode path keys stay `(0, utc_instant_key(...))`). When emitting `TableRow`, map the label to `Option<String>`:

```rust
bucket_key: (params.bucket != TableBucket::None).then_some(label),
```

(the `TableBucket::None` label is the empty string internally, matching the stats none path, so confidence roll-up keys stay consistent).
5. In `signal_relationship.rs`, the two `row.bucket_key` reads become `row.bucket_key.as_deref().unwrap_or("")` (relationship never requests `TableBucket::None`; Task 5 tightens this).

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p chartpds-core` — all green (workspace still builds; the MCP tool layer doesn't name these new variants yet).

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/queries/aligned_table.rs crates/chartpds-core/src/queries/signal_relationship.rs
git commit -m "issue #29: table gains none/hour/day_of_week buckets, nullable bucket_key"
```

---

### Task 3: `longest_run_in_range` column aggregate

**Files:**
- Modify: `crates/chartpds-core/src/queries/episodes.rs` (add `runs`, moved from `longest_continuous_in_value_range.rs:60-89`)
- Modify: `crates/chartpds-core/src/queries/aligned_table.rs`

**Interfaces:**
- Produces: `ColumnAggregate::LongestRunInRange { value_min: f64, value_max: f64, gap_seconds: i64 }`; `pub(crate) fn runs(intervals: &[(OffsetDateTime, OffsetDateTime)], gap_seconds: i64) -> Vec<Run>` with `pub(crate) struct Run { pub(crate) start: OffsetDateTime, pub(crate) minutes: f64 }` in `episodes.rs`.
- Semantics (preserved from the retiring tool, documented on the variant): runs are chained over the **whole window's** in-range intervals of the column's coding, then each run is attributed **whole** to the bucket containing its start (a midnight-crossing run stays in one day row). A bucket with interval rows but no run starting in it reads `0.0`; a bucket with no interval rows of the coding reads `null`.

- [ ] **Step 1: Write the failing tests** (in `aligned_table.rs` tests)

```rust
fn hr_interval(start: OffsetDateTime, end: OffsetDateTime, v: f64) -> IntervalObsSpec {
    IntervalObsSpec {
        coding_system: SYSTEM_LOINC,
        coding_code: "8867-4",
        effective_start: start,
        effective_end: end,
        value_quantity: v,
    }
}

#[tokio::test]
async fn longest_run_chains_across_gap_and_attributes_to_start_day() {
    // 23:50–00:00 and 00:05–00:15 next day, gap 5 min, all in range:
    // with gap_seconds 300 this is ONE 25-minute run, attributed to Jan 1.
    let (pool, _) = seed_interval_observations(&[
        hr_interval(
            datetime!(2026-01-01 23:50:00 UTC),
            datetime!(2026-01-02 00:00:00 UTC),
            110.0,
        ),
        hr_interval(
            datetime!(2026-01-02 00:05:00 UTC),
            datetime!(2026-01-02 00:15:00 UTC),
            110.0,
        ),
    ])
    .await;
    let columns = [ColumnSpec {
        coding_system: SYSTEM_LOINC,
        coding_code: "8867-4",
        aggregate: ColumnAggregate::LongestRunInRange {
            value_min: 100.0,
            value_max: 120.0,
            gap_seconds: 300,
        },
        field: StatsField::Value,
    }];
    let table = aligned_table(&pool, NOW, day_params(&columns))
        .await
        .expect("query");
    // Jan 1 row: the whole 25-minute run. Jan 2 row: has interval rows but
    // no run STARTING there -> 0.0, not null.
    assert_eq!(table.rows[0].bucket_key.as_deref(), Some("2026-01-01"));
    assert_eq!(table.rows[0].values, vec![Some(25.0)]);
    assert_eq!(table.rows[1].bucket_key.as_deref(), Some("2026-01-02"));
    assert_eq!(table.rows[1].values, vec![Some(0.0)]);
}

#[tokio::test]
async fn longest_run_out_of_range_rows_break_runs() {
    // in-range, out-of-range, in-range back to back: two 10-minute runs, not one.
    let (pool, _) = seed_interval_observations(&[
        hr_interval(
            datetime!(2026-01-01 08:00:00 UTC),
            datetime!(2026-01-01 08:10:00 UTC),
            110.0,
        ),
        hr_interval(
            datetime!(2026-01-01 08:10:00 UTC),
            datetime!(2026-01-01 08:20:00 UTC),
            130.0,
        ),
        hr_interval(
            datetime!(2026-01-01 08:20:00 UTC),
            datetime!(2026-01-01 08:30:00 UTC),
            110.0,
        ),
    ])
    .await;
    let columns = [ColumnSpec {
        coding_system: SYSTEM_LOINC,
        coding_code: "8867-4",
        aggregate: ColumnAggregate::LongestRunInRange {
            value_min: 100.0,
            value_max: 120.0,
            gap_seconds: 0,
        },
        field: StatsField::Value,
    }];
    let table = aligned_table(&pool, NOW, day_params(&columns))
        .await
        .expect("query");
    assert_eq!(table.rows[0].values, vec![Some(10.0)]);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p chartpds-core longest_run`
Expected: FAIL — no `LongestRunInRange` variant.

- [ ] **Step 3: Implement**

1. Move `runs` + `Run` verbatim from `longest_continuous_in_value_range.rs` into `episodes.rs` as `pub(crate)` (leave the old module compiling by importing from `episodes` — it is deleted in Task 6).
2. Add the `ColumnAggregate::LongestRunInRange { value_min, value_max, gap_seconds }` variant.
3. In `column_cells`, longest-run needs a window-wide pass, so branch before the per-row loop: when the column's aggregate is `LongestRunInRange`, still iterate rows to record `contributions`, bucket labels, `rows_seen`, and `intervals_seen` (any row with `effective_end`), but additionally collect the window's in-range intervals `(start, end)` in fetch order. After the loop, compute `runs(&in_range, gap_seconds)` and for each run resolve its bucket label (same `episodes`/`calendar_label` logic applied to `run.start`) and update that cell: `cell.longest_minutes = cell.longest_minutes.max(run.minutes)`.
4. `Cell` gains `longest_minutes: f64` (default 0.0); `Cell::reduce` gains:

```rust
ColumnAggregate::LongestRunInRange { .. } => {
    self.intervals_seen.then_some(self.longest_minutes)
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p chartpds-core`
Expected: PASS, workspace-wide.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/queries/episodes.rs crates/chartpds-core/src/queries/aligned_table.rs crates/chartpds-core/src/queries/longest_continuous_in_value_range.rs
git commit -m "issue #29: longest_run_in_range column aggregate with whole-run attribution"
```

---

### Task 4: explicit episode spec for `observation_stats`

**Files:**
- Modify: `crates/chartpds-core/src/queries/observation_stats.rs`
- Modify: `crates/chartpds-mcp/src/server.rs` (call-site shim only; the tool's public args change in Task 7)

**Interfaces:**
- Produces: `ObservationStatsParams` loses `gap_seconds: i64` and gains `episode: Option<EpisodeSpec<'a>>`; new error variant `ObservationStatsError::MissingEpisodeSpec` (message: `bucket "episode" requires an episode spec`). Episode detection now fetches the **episode coding's** intervals via `fetch_all_intervals` (identical to `aligned_table::detect_bucket_episodes`), no longer chaining the aggregated coding's own rows.

- [ ] **Step 1: Write the failing test** (in `observation_stats.rs` tests; adapt the existing episode-bucket test to the new param and add the error case)

```rust
#[tokio::test]
async fn episode_bucket_without_spec_is_an_error() {
    let (pool, _) = seed_observations(&[]).await;
    let err = observation_stats(
        &pool,
        NOW,
        ObservationStatsParams {
            coding_system: SYSTEM_LOINC,
            coding_code: "8867-4",
            start: datetime!(2026-01-01 00:00:00 UTC),
            end: datetime!(2026-02-01 00:00:00 UTC),
            field: StatsField::Value,
            bucket: StatsBucket::Episode,
            timezone: None,
            thresholds: None,
            episode: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ObservationStatsError::MissingEpisodeSpec));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p chartpds-core observation_stats`
Expected: FAIL — no `episode` field / variant.

- [ ] **Step 3: Implement**

Params change as specified. In `observation_stats`, replace the self-chaining episode block with:

```rust
let episodes = match (params.bucket == StatsBucket::Episode, &params.episode) {
    (false, _) => None,
    (true, None) => return Err(ObservationStatsError::MissingEpisodeSpec),
    (true, Some(spec)) => {
        let all = fetch_all_intervals(
            pool,
            spec.coding_system,
            spec.coding_code,
            params.start,
            params.end,
        )
        .await?;
        let intervals: Vec<(OffsetDateTime, OffsetDateTime)> =
            all.iter().map(|r| (r.start, r.end)).collect();
        Some(detect_episodes(&intervals, spec.gap_seconds))
    }
};
```

(`EpisodeSpec` is imported from `aligned_table`; existing episode tests update to pass `episode: Some(EpisodeSpec { coding_system: <same coding>, coding_code: <same>, gap_seconds })` — behavior for interval codings is unchanged.) In `server.rs`'s `observation_stats` tool, keep the current public args compiling by building the spec from the existing fields: `episode: (bucket == StatsBucket::Episode).then(|| EpisodeSpec { coding_system: &args.coding.system, coding_code: &args.coding.code, gap_seconds })` — a temporary shim Task 7 removes.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p chartpds-core && cargo test -p chartpds-mcp`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/queries/observation_stats.rs crates/chartpds-mcp/src/server.rs
git commit -m "issue #29: stats episode bucketing takes an explicit episode spec"
```

---

### Task 5: relationship gains `hour` and `episode` buckets

**Files:**
- Modify: `crates/chartpds-core/src/queries/signal_relationship.rs`

**Interfaces:**
- Consumes: `TableBucket::Hour`, `EpisodeSpec`, Task 1 hour labels.
- Produces: `RelationshipBucket::{Hour, Day, Week, Month, Episode}`; `SignalRelationshipParams` gains `episode: Option<EpisodeSpec<'a>>` (passed through to `aligned_table`; `MissingEpisodeSpec` surfaces from there). Lag semantics: calendar buckets shift by key arithmetic (hour = instant + `lag`·3600 s, relabelled in the request timezone via `bucket_key(.., StatsBucket::Hour, tz)` — parse the key with `OffsetDateTime::parse(&key, &Rfc3339)`); episode buckets pair by **row index** (`x` at row `i` with `y` at row `i + lag`), i.e. lag 1 = the next episode.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test]
async fn episode_bucket_pairs_by_episode_index_with_lag() {
    // Three sleep episodes; x = total-sleep summary (93832-4), y = same.
    // With lag 1, episode i's x pairs with episode i+1's y: 2 pairs.
    let night = |d: i64, minutes: f64| IntervalObsSpec {
        coding_system: SYSTEM_LOINC,
        coding_code: "93832-4",
        effective_start: datetime!(2026-01-01 23:00:00 UTC) + time::Duration::days(d),
        effective_end: datetime!(2026-01-02 06:00:00 UTC) + time::Duration::days(d),
        value_quantity: minutes,
    };
    let (pool, _) = seed_interval_observations(&[
        night(0, 400.0),
        night(1, 410.0),
        night(2, 420.0),
    ])
    .await;
    let col = ColumnSpec {
        coding_system: SYSTEM_LOINC,
        coding_code: "93832-4",
        aggregate: ColumnAggregate::Mean,
        field: StatsField::Value,
    };
    let result = signal_relationship(
        &pool,
        NOW,
        SignalRelationshipParams {
            x: col,
            y: col,
            start: datetime!(2026-01-01 00:00:00 UTC),
            end: datetime!(2026-01-10 00:00:00 UTC),
            bucket: RelationshipBucket::Episode,
            timezone: None,
            lag_buckets: 1,
            x_threshold: None,
            episode: Some(EpisodeSpec {
                coding_system: SYSTEM_LOINC,
                coding_code: "93832-4",
                gap_seconds: 0,
            }),
        },
    )
    .await
    .expect("query");
    assert_eq!(result.n_pairs, 2);
}

#[tokio::test]
async fn hour_bucket_lag_pairs_adjacent_hours() {
    // x at 08:xx and y at 09:xx; lag 1 pairs them.
    let (pool, _) = seed_interval_observations(&[
        hr_interval(
            datetime!(2026-01-01 08:00:00 UTC),
            datetime!(2026-01-01 08:05:00 UTC),
            100.0,
        ),
        hr_interval(
            datetime!(2026-01-01 09:00:00 UTC),
            datetime!(2026-01-01 09:05:00 UTC),
            60.0,
        ),
    ])
    .await;
    let col = ColumnSpec {
        coding_system: SYSTEM_LOINC,
        coding_code: "8867-4",
        aggregate: ColumnAggregate::Mean,
        field: StatsField::Value,
    };
    let result = signal_relationship(
        &pool,
        NOW,
        SignalRelationshipParams {
            x: col,
            y: col,
            start: datetime!(2026-01-01 00:00:00 UTC),
            end: datetime!(2026-01-02 00:00:00 UTC),
            bucket: RelationshipBucket::Hour,
            timezone: None,
            lag_buckets: 1,
            x_threshold: None,
            episode: None,
        },
    )
    .await
    .expect("query");
    assert_eq!(result.n_pairs, 1);
}
```

(Reuse Task 3's `hr_interval` helper — move it into `test_support.rs` if that's cleaner than duplicating; the aligned_table copy then imports it too.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p chartpds-core signal_relationship`
Expected: FAIL — missing variants/field.

- [ ] **Step 3: Implement**

- Add variants + `episode` field; map `RelationshipBucket::Hour -> TableBucket::Hour`, `Episode -> TableBucket::Episode`, pass `params.episode` through.
- Pairing: for `Episode`, replace key-shift with index pairing:

```rust
if params.bucket == RelationshipBucket::Episode {
    for (i, row) in table.rows.iter().enumerate() {
        let Some(x) = row.values[0] else { continue };
        let Some(j) = i64::try_from(i)
            .ok()
            .and_then(|i| i.checked_add(params.lag_buckets))
            .and_then(|j| usize::try_from(j).ok())
        else {
            continue;
        };
        if let Some(Some(y)) = table.rows.get(j).map(|r| r.values[1]) {
            pairs.push((x, y));
        }
    }
}
```

- `shift_bucket_key` gains a `Hour` arm (needs the resolved `TimeZone` — thread it in): parse key as RFC 3339, add `Duration::hours(lag)`, relabel with `bucket_key(shifted, StatsBucket::Hour, tz)`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p chartpds-core`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/queries/signal_relationship.rs crates/chartpds-core/src/queries/test_support.rs crates/chartpds-core/src/queries/aligned_table.rs
git commit -m "issue #29: relationship gains hour and episode buckets"
```

---

### Task 6: retire the two standalone in-range tools and their core modules

> From this task on, holdout tests fail. That is the expected state until the human blesses redrafted holdout tests after the plan completes. Verification switches to `cargo test --workspace --exclude holdout` + the lint/fmt/sqlx gates.

**Files:**
- Delete: `crates/chartpds-core/src/queries/duration_in_value_range.rs`, `crates/chartpds-core/src/queries/longest_continuous_in_value_range.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs` (drop the two `mod`s and their `pub use` blocks)
- Modify: `crates/chartpds-mcp/src/server.rs` (delete `observation_duration_in_range` + `observation_longest_period_in_range` tools, their args structs, and their tests)

**Interfaces:**
- Consumes: Tasks 2–3 (the capability now lives in `observation_table` columns).
- Produces: neither tool nor query exists; `queries::` exports no `Bucket`, `LongestBucket`, `DurationInRange*`, `LongestContinuous*` names.

- [ ] **Step 1: Port the irreplaceable known-answer tests.** Before deleting, read both modules' `#[cfg(test)]` sections and port any behavior not already covered by Tasks 2–5 tests into `aligned_table.rs` tests, expressed via `DurationInRange`/`LongestRunInRange` columns (candidates: interval credited whole to its start bucket; value-boundary inclusivity of `value_min`/`value_max`; hour-bucket DST fall-back keying if present). Do not port tests for the retired result shapes themselves.

- [ ] **Step 2: Delete** the two files, their `mod.rs` lines, the two tool functions, their arg structs, and their server tests.

- [ ] **Step 3: Regenerate the sqlx cache** (the retired modules' queries leave stale entries):

Run: `just prepare-sql`

- [ ] **Step 4: Verify**

Run: `env -u RUSTUP_TOOLCHAIN cargo test --workspace --exclude holdout && just lint && cargo sqlx prepare --check`
Expected: PASS. Then run `cargo test -p holdout` and confirm failures are confined to tests that call the two deleted tools (currently in `intraday_hour_bucketing.rs`, `fitbit_confidence.rs`, `fitbit_hr_dedup.rs`, `episode_bucketing.rs`) — report the failing list in the task summary, do not touch them.

- [ ] **Step 5: Commit**

```bash
git add -A crates/ .sqlx/
git commit -m "issue #29: retire standalone in-range tools; capability lives in observation_table"
```

---

### Task 7: shared server vocabulary — parsing helpers and the stats tool reshaped

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs`

**Interfaces:**
- Produces (used by every remaining task):

```rust
/// Episode definition shared by stats/table/relationship args.
/// (Rename of TableEpisodeArgs; same shape.)
pub(crate) struct EpisodeArgs { pub(crate) coding: Coding, pub(crate) gap_seconds: Option<i64> }

/// Optional half-open window with open-ended defaults.
/// start default: 0001-01-01T00:00:00Z; end default: `now`.
fn parse_window(start: Option<&str>, end: Option<&str>, now: OffsetDateTime)
    -> Result<(OffsetDateTime, OffsetDateTime), McpError>;

/// Parse a bucket string against the tool's allowed subset; error lists the
/// allowed values verbatim.
fn parse_bucket(s: Option<&str>, default: &str, allowed: &[&str]) -> Result<String, McpError>;

/// Validate + convert EpisodeArgs (gap_seconds >= 0, default 0).
fn parse_episode(args: Option<&EpisodeArgs>, bucket_is_episode: bool)
    -> Result<Option<chartpds_core::queries::EpisodeSpec<'_>>, McpError>;
```

  plus `parse_column` extended with `longest_run_in_range` (requires `value_min`/`value_max`; accepts `gap_seconds >= 0` default 0; `gap_seconds` invalid with any other aggregate) and the new validation `value_min <= value_max` for both range aggregates. `TableColumnArgs` gains `pub(crate) gap_seconds: Option<i64>`.
- The `observation_stats` tool's public args become:

```rust
pub(crate) struct ObservationStatsArgs {
    pub(crate) coding: Coding,
    pub(crate) start: Option<String>,   // was required
    pub(crate) end: Option<String>,     // was required
    pub(crate) field: Option<String>,
    pub(crate) bucket: Option<String>,  // none|hour|day|week|month|day_of_week|episode
    pub(crate) timezone: Option<String>,
    pub(crate) thresholds: Option<Vec<f64>>,
    pub(crate) episode: Option<EpisodeArgs>, // replaces top-level gap_seconds
}
```

  and its bucketed result serializes `{"items": [...]}` — rename the core field `ObservationStats::Buckets { per_bucket }` to `{ items }` in `observation_stats.rs` (update its tests).

- [ ] **Step 1: Write the failing tests** (server tests; construct the server directly as the existing tests do)

```rust
#[tokio::test]
async fn observation_stats_defaults_open_window() {
    let server = fresh_server_with_one_weight().await;
    let result = server
        .observation_stats(Parameters(ObservationStatsArgs {
            coding: Coding {
                system: "http://loinc.org".to_owned(),
                code: "29463-7".to_owned(),
            },
            start: None,
            end: None,
            field: None,
            bucket: None,
            timezone: None,
            thresholds: None,
            episode: None,
        }))
        .await
        .expect("tool call");
    let text = match &result.content[0].raw {
        rmcp::model::RawContent::Text(t) => &t.text,
        _ => panic!("expected text content"),
    };
    let v: serde_json::Value = serde_json::from_str(text).expect("json");
    assert_eq!(v["count"], 1);
}

#[tokio::test]
async fn observation_stats_bucketed_result_uses_items() {
    let server = fresh_server_with_one_weight().await;
    let result = server
        .observation_stats(Parameters(ObservationStatsArgs {
            coding: Coding {
                system: "http://loinc.org".to_owned(),
                code: "29463-7".to_owned(),
            },
            start: None,
            end: None,
            field: None,
            bucket: Some("day".to_owned()),
            timezone: None,
            thresholds: None,
            episode: None,
        }))
        .await
        .expect("tool call");
    let text = match &result.content[0].raw {
        rmcp::model::RawContent::Text(t) => &t.text,
        _ => panic!("expected text content"),
    };
    let v: serde_json::Value = serde_json::from_str(text).expect("json");
    assert_eq!(v["items"][0]["bucket_key"], "2026-01-01");
}

#[tokio::test]
async fn observation_stats_episode_bucket_requires_episode_object() {
    let server = fresh_server_with_one_weight().await;
    let err = server
        .observation_stats(Parameters(ObservationStatsArgs {
            coding: Coding {
                system: "http://loinc.org".to_owned(),
                code: "29463-7".to_owned(),
            },
            start: None,
            end: None,
            field: None,
            bucket: Some("episode".to_owned()),
            timezone: None,
            thresholds: None,
            episode: None,
        }))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("episode"));
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test -p chartpds-mcp observation_stats`. Expected: FAIL (args shape).

- [ ] **Step 3: Implement.** Helpers as specified (window sentinel: `time::macros::datetime!(0001-01-01 0:00 UTC)`); rework the stats tool to use them (`hour` joins its bucket subset; episode spec borrows from `args.episode`, replacing the Task 4 shim; `episode` present with a non-episode bucket → `invalid_params` "episode is only valid with bucket \"episode\"", matching the table tool). Rename the core `per_bucket` field to `items`. Update the tool description to document: optional start/end open-ended defaults, the full bucket list, the episode object, `{items}` bucketed shape.

- [ ] **Step 4: Run to verify pass** — `cargo test -p chartpds-mcp && cargo test -p chartpds-core`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-mcp/src/server.rs crates/chartpds-core/src/queries/observation_stats.rs
git commit -m "issue #29: shared arg vocabulary; observation_stats reshaped"
```

---

### Task 8: `observation_table` and `observation_relationship` reshaped

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs`

**Interfaces:**
- Consumes: Task 7 helpers.
- `ObservationTableArgs`: `start`/`end` become `Option<String>`; `episode` field type becomes `EpisodeArgs`; bucket subset `none|hour|day|week|month|day_of_week|episode` (default `day`).
- `ObservationRelationshipArgs`: `start`/`end` become `Option<String>`; gains `episode: Option<EpisodeArgs>`; bucket subset `hour|day|week|month|episode` (default `day`).
- Column parsing (both tools) accepts `longest_run_in_range` per Task 7.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test]
async fn observation_table_none_bucket_yields_null_key_row() {
    let server = fresh_server_with_one_weight().await;
    let result = server
        .observation_table(Parameters(ObservationTableArgs {
            columns: vec![TableColumnArgs {
                coding: Coding {
                    system: "http://loinc.org".to_owned(),
                    code: "29463-7".to_owned(),
                },
                aggregate: None,
                field: None,
                value_min: None,
                value_max: None,
                gap_seconds: None,
            }],
            start: None,
            end: None,
            bucket: Some("none".to_owned()),
            timezone: None,
            episode: None,
        }))
        .await
        .expect("tool call");
    let text = match &result.content[0].raw {
        rmcp::model::RawContent::Text(t) => &t.text,
        _ => panic!("expected text content"),
    };
    let v: serde_json::Value = serde_json::from_str(text).expect("json");
    assert!(v["rows"][0]["bucket_key"].is_null());
    assert_eq!(v["rows"][0]["values"][0], 72.5);
}

#[tokio::test]
async fn observation_table_rejects_longest_run_without_bounds() {
    let server = fresh_server_with_one_weight().await;
    let err = server
        .observation_table(Parameters(ObservationTableArgs {
            columns: vec![TableColumnArgs {
                coding: Coding {
                    system: "http://loinc.org".to_owned(),
                    code: "29463-7".to_owned(),
                },
                aggregate: Some("longest_run_in_range".to_owned()),
                field: None,
                value_min: None,
                value_max: None,
                gap_seconds: None,
            }],
            start: None,
            end: None,
            bucket: None,
            timezone: None,
            episode: None,
        }))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("value_min"));
}
```

Also add an `observation_relationship` test passing `bucket: Some("episode")` with an `episode` object over two seeded sleep summaries (assert `n_pairs` = 1 with `lag: Some(1)` and three episodes, mirroring the Task 5 core test through the tool layer).

- [ ] **Step 2: Run to verify failure** — `cargo test -p chartpds-mcp observation_table`. Expected: FAIL.

- [ ] **Step 3: Implement.** Rework both tools onto `parse_window`/`parse_bucket`/`parse_episode`/extended `parse_column`; `aggregate_name` gains the `LongestRunInRange` arm (`"longest_run_in_range"`). Update both tool descriptions: full bucket lists, optional open-ended start/end, the two range aggregates (semantics: `duration_in_range` = minutes in range, interval credited whole to its start bucket; `longest_run_in_range` = longest unbroken in-range run in minutes, run attributed whole to the bucket containing its start, per-column `gap_seconds` = allowed gap before a run breaks), episode lag = next-episode pairing, `bucket_key` `null` for `none`.

- [ ] **Step 4: Run to verify pass** — `cargo test -p chartpds-mcp`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-mcp/src/server.rs
git commit -m "issue #29: observation_table and observation_relationship reshaped"
```

---

### Task 9: observation read tools — `observation_latest`, `observation_history`, `observation_codings`, `coding_definitions`

**Files:**
- Modify: `crates/chartpds-core/src/queries/latest_by_code.rs` → rename file to `latest_by_coding.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs`
- Modify: `crates/chartpds-mcp/src/server.rs`

**Interfaces:**
- Core: `pub async fn latest_by_coding(pool: &SqlitePool, now: OffsetDateTime, coding_system: &str, coding_code: &str) -> Result<Option<ObservationWithConfidence>, sqlx::Error>` — SQL gains `WHERE coding_system = ? AND coding_code = ?`.
- Tools:
  - `observation_latest` (was `latest_observation_by_code`): args `{coding: Coding}`; returns the observation object or `null`.
  - `observation_history` (was `get_observation_history`): args `{codings: Vec<Coding>, start: Option<String>, end: Option<String>}` (was `since`/`until`); returns `{"items": [...]}`.
  - `observation_codings` (was `observation_counts`): returns `{"items": [...]}`.
  - `coding_definitions` (was `describe_codings`): returns `{"items": [...]}`.
  - The unused-args placeholder struct renames `ObservationCountsArgs` → `EmptyArgs` (doc: "Arguments for tools that take none.").

- [ ] **Step 1: Write the failing tests.** Rename/adjust the six existing server tests for these tools: `observation_latest` called with `Coding {system: "http://loinc.org", code: "29463-7"}` asserts `value_quantity == 72.5`; with a bogus *system* and real code asserts `"null"` (proves the system filter); `observation_history` passes `start`/`end: None` and asserts `v["items"]` is a 1-element array; `observation_codings` asserts `v["items"][0]["coding_code"] == "29463-7"`; `coding_definitions` asserts `v["items"][0]["coding_code"] == "aasm-sleep-stage"`.

- [ ] **Step 2: Run to verify failure** — `cargo test -p chartpds-mcp`. Expected: FAIL (names/shapes).

- [ ] **Step 3: Implement.** Core rename + SQL change, then `just prepare-sql`. Server: rename the four tool fns and arg structs, wrap list results as `serde_json::json!({"items": rows})`, route `observation_history` bounds through `parse_window`… **no** — history's core fn takes `Option<OffsetDateTime>` natively, so parse each bound when present and pass the `Option`s straight through (no sentinel needed). Update all four descriptions (`observation_codings` keeps the discovery guidance; `observation_history` documents optional open-ended `start`/`end`).

- [ ] **Step 4: Run to verify pass** — `env -u RUSTUP_TOOLCHAIN cargo test --workspace --exclude holdout && cargo sqlx prepare --check`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/queries/ crates/chartpds-mcp/src/server.rs .sqlx/
git commit -m "issue #29: observation read tools renamed; latest takes a coding"
```

---

### Task 10: clinical + narrative tools — `problem_list`, `medication_list`, `notification_list`, `narrative_search`, `narrative_get`

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs`

**Interfaces:**
- `problem_list` / `medication_list` (renames; response shape already `{latest_document_date, items}` — unchanged).
- `notification_list` (was `list_notifications`): returns `{"items": [...]}`.
- `narrative_search` (was `search_narratives`): returns `{"items": [...]}`; error mapping becomes:

```rust
.map_err(|err| match &err {
    sqlx::Error::Database(db) if db.message().contains("fts5") => {
        McpError::invalid_params(format!("invalid FTS5 query: {err}"), None)
    }
    _ => McpError::internal_error(format!("query failed: {err}"), None),
})
```

- `narrative_get` (was `get_narrative`): arg struct field renames `document_id` → `source_document_id`; returns the object or `null` (unchanged).

- [ ] **Step 1: Write the failing tests.** Rename the existing tests; `notification_list` asserts `v["items"][0]["condition_id"] == "auth_expired:fitbit"`; add a `narrative_search` test with `query: Some("AND AND".to_owned())` on an empty store asserting the error is `invalid_params` (rmcp error code `-32602`) and one with a valid query asserting `v["items"]` is an array.

- [ ] **Step 2: Run to verify failure** — `cargo test -p chartpds-mcp`. Expected: FAIL.

- [ ] **Step 3: Implement** the renames, `{items}` wraps, error split, and description updates (problem/medication descriptions keep the `status`-unreliability warning verbatim).

- [ ] **Step 4: Run to verify pass** — `cargo test -p chartpds-mcp`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-mcp/src/server.rs
git commit -m "issue #29: clinical/narrative/notification tools renamed and normalized"
```

---

### Task 11: write-side tools — `record_ingest`, `source_connect`, `source_sync`, `index_rebuild`

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs`

**Interfaces:**
- `record_ingest` (was `ingest_record`): args and behavior unchanged.
- `source_connect` (was `connect_source`): returns JSON —
  - fitbit: `{"source": "fitbit", "status": "authorization_pending", "authorization_url": "<url>", "message": "Open the URL in a browser to authorize; the server catches the callback and stores credentials automatically."}`
  - oura: `{"source": "oura", "status": "connected", "message": "Oura credentials stored. Call source_sync with source=\"oura\"."}`
- `source_sync` (was `sync_source`): top-level key `results` → `items`.
- `index_rebuild` (was `rebuild_index`): description's return-shape sentence becomes `Returns {blobs_found, ccda_ingested, fitbit_ingested, oura_ingested, narratives_ingested, extractions_applied, blobs_skipped}.`
- `get_info` instructions update to name the noun-first families: `"ChartPDS personal data store. Tool families: observation_* (query), coding_definitions, problem_list/medication_list, narrative_* (documents), record_ingest, source_* (adapters), notification_list, index_rebuild."`

- [ ] **Step 1: Write the failing tests.** Rename the ingest/rebuild tests to the new tool names; add a `source_connect` oura test asserting `v["status"] == "connected"` and that a `source_sync` `{source: Some("oura"), ...}` call afterwards returns `v["items"][0]["source"] == "oura"`.

- [ ] **Step 2: Run to verify failure** — `cargo test -p chartpds-mcp`. Expected: FAIL.

- [ ] **Step 3: Implement.** Renames, JSON envelope for `source_connect`, `items` key in the sync payload and in `sync_fitbit_structured`/`sync_oura_structured` messages that mention tool names (`connect_source` → `source_connect` in user-facing message strings, including `resolve_oura_token`'s error), description updates.

- [ ] **Step 4: Run to verify pass** — `env -u RUSTUP_TOOLCHAIN cargo test --workspace --exclude holdout && just lint`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-mcp/src/server.rs
git commit -m "issue #29: write-side tools renamed; source_connect returns JSON"
```

---

### Task 12: documentation + final verification + handoff

**Files:**
- Modify: `CLAUDE.md` (MCP server section: tool count + list + descriptions; Queries section: remove `duration_in_value_range` / `longest_continuous_in_value_range`, note the column aggregates, the explicit episode spec, `latest_by_coding`, optional bounds)
- Modify: `docs/superpowers/specs/2026-07-17-mcp-tool-surface-consolidation-design.md` (Status: Approved → Implemented)

- [ ] **Step 1: Update CLAUDE.md.** Rewrite the "serves 18 tools" list to the 16-tool catalog with one-line descriptions matching the new names/shapes; update the Queries paragraph (the two-tool day-attribution caveat paragraph is replaced by a sentence on the two column aggregates' attribution semantics: `duration_in_range` credits each interval whole to its start bucket; `longest_run_in_range` attributes a whole run to the bucket containing its start).

- [ ] **Step 2: Full verification.**

Run: `env -u RUSTUP_TOOLCHAIN cargo test --workspace --exclude holdout && just fmt-check && just lint && cargo sqlx prepare --check && just holdout-verify`
Expected: ALL PASS (`holdout-verify` passes because no protected file was touched).

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p holdout 2>&1 | tail -30`
Expected: FAILURES ONLY from renamed/deleted tool calls. Record the failing test list verbatim.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md docs/superpowers/specs/2026-07-17-mcp-tool-surface-consolidation-design.md
git commit -m "issue #29: docs for the consolidated 16-tool surface"
```

- [ ] **Step 4: Hand off.** Report to the human: implementation complete on `worktree-issue-29-tool-surface`; the verbatim holdout failure list; next step is the human explicitly asking for a holdout-update draft (staged-uncommitted) to bless via `just holdout-bless`, after which the branch can merge via PR.

---

## Post-plan (human-gated, NOT part of this plan)

Holdout redraft: only when the human explicitly asks. Scope known today: mechanical renames in all 11 holdout files; rewrites for `observation_duration_in_range` calls against `observation_table` (`hour` bucket + `duration_in_range`/`longest_run_in_range` columns); `since`/`until` → `start`/`end`; `per_bucket`/bare-array assertions → `items`; `document_id` → `source_document_id`; stats episode calls gain the `episode` object.
