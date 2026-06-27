# P2 — Discovery, Coding Self-Description, Multi-Coding History, WASO Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enrich metric discovery, add a self-description catalog for ChartPDS-minted codings, replace single-code history with a multi-coding/open-bounds tool, and emit a per-night WASO observation — all behind a generic, coding-agnostic tool surface.

**Architecture:** Four mostly-independent verticals. #6 enriches the existing `counts_per_code` query + `observation_counts` tool. #6b adds a static catalog in `clinical` derived from the `AasmSleepStage` enum, surfaced by a new `describe_codings` tool. #7 adds an `observation_history` query (multi-coding, optional bounds) and swaps the `observations_in_range` tool for `get_observation_history`, deleting the old `in_range` query. #9 adds a WASO parser function in the Oura adapter and emits one LOINC `103215-0` observation per night from `storage.rs`.

**Tech Stack:** Rust, sqlx (offline/compile-time-checked SQLite), rmcp (MCP server), `time` crate (RFC 3339), `just` task runner.

## Global Constraints

- **Lint policy:** never bypass a lint. Every `pub` item needs a doc comment (`missing_docs` + `-D warnings`). Any `#[allow(...)]` requires a `reason = "..."` string.
- **Module boundaries:** `chartpds-core` default visibility is `pub(crate)`; items the binary calls must be `pub` and re-exported through `lib.rs` / module `mod.rs` files. The binary calls only `chartpds_core::{queries,clinical}::*` re-exports.
- **SQL changes:** after editing any `migrations/*.sql` or `sqlx::query!`/`query_as!`, run `just prepare-sql` and commit the `.sqlx/` cache update in the same commit. `just check` runs `cargo sqlx prepare --check`.
- **Migration policy:** forward-only. (No migration is needed in this plan.)
- **Done gate:** `just check` (chains `fmt-check`, `lint`, `typecheck`, `test`, `cargo deny`, `cargo machete`) must pass before declaring complete.
- **Author commits** as `Francis Hwang <sera@fhwang.net>`.
- **Open-source repo:** keep private-harness specifics out of code, comments, commits. Frame in general terms ("external MCP clients", "an agentic client").

---

## File Structure

- `crates/chartpds-core/src/queries/counts_per_code.rs` — **modify** (#6): grow struct + SQL.
- `crates/chartpds-core/src/queries/mod.rs` — **modify** (#6, #7): re-exports.
- `crates/chartpds-core/src/clinical/aasm.rs` — **modify** (#6b): add `ALL`, `as_str`, `label`, `discriminant`.
- `crates/chartpds-core/src/clinical/catalog.rs` — **create** (#6b): minted-coding catalog.
- `crates/chartpds-core/src/clinical/mod.rs` — **modify** (#6b): declare + re-export catalog.
- `crates/chartpds-core/src/queries/observation_history.rs` — **create** (#7).
- `crates/chartpds-core/src/queries/in_range.rs` — **delete** (#7).
- `crates/chartpds-core/src/sources/oura/parser.rs` — **modify** (#9): WASO parser.
- `crates/chartpds-core/src/clinical/coding.rs` — **modify** (#9): `LOINC_WASO` constant.
- `crates/chartpds-core/src/sources/oura/storage.rs` — **modify** (#9): emit WASO.
- `crates/chartpds-mcp/src/server.rs` — **modify** (#6, #6b, #7): tool descriptions, new tools, removed tool, tests.
- `CLAUDE.md` — **modify** (final): tool list + module docs.

---

## Task 1: #6 — Enrich `observation_counts` (system + date span)

**Files:**
- Modify: `crates/chartpds-core/src/queries/counts_per_code.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs`
- Modify: `crates/chartpds-mcp/src/server.rs` (tool description + one test)

**Interfaces:**
- Produces: `chartpds_core::queries::{counts_per_code, MetricSummary}`. `MetricSummary { coding_system: String, coding_code: String, count: i64, first_effective_start: OffsetDateTime, last_effective_start: OffsetDateTime }`. `counts_per_code(&SqlitePool) -> Result<Vec<MetricSummary>, sqlx::Error>`.

- [ ] **Step 1: Rewrite the struct, SQL, and tests in `counts_per_code.rs`**

Replace the whole file with:

```rust
//! Per-coding discovery: which codings exist, how many, over what span.

use sqlx::SqlitePool;
use time::OffsetDateTime;

/// One discovered coding present in the observations table.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct MetricSummary {
    /// FHIR coding system URI (e.g. `"http://loinc.org"` or the AASM URI).
    pub coding_system: String,
    /// Coding code within the system (e.g. `"29463-7"`).
    pub coding_code: String,
    /// Number of observation rows for this `(system, code)`.
    pub count: i64,
    /// Earliest `effective_start` for this coding (RFC 3339 on the wire).
    #[serde(with = "time::serde::rfc3339")]
    pub first_effective_start: OffsetDateTime,
    /// Latest `effective_start` for this coding (RFC 3339 on the wire).
    #[serde(with = "time::serde::rfc3339")]
    pub last_effective_start: OffsetDateTime,
}

/// Discover the codings present in the store, grouped by `(system, code)`.
///
/// Returns one [`MetricSummary`] per distinct `(coding_system, coding_code)`,
/// ordered by system then code. `count` is the row count; `first/last_
/// effective_start` are the `MIN`/`MAX` of `effective_start` (lexical over the
/// stored RFC 3339 text — the same ordering assumption the other queries use).
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn counts_per_code(pool: &SqlitePool) -> Result<Vec<MetricSummary>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT coding_system,
               coding_code AS "coding_code!: String",
               COUNT(*)    AS "count!: i64",
               MIN(effective_start) AS "first_effective_start!: OffsetDateTime",
               MAX(effective_start) AS "last_effective_start!: OffsetDateTime"
        FROM observations
        GROUP BY coding_system, coding_code
        ORDER BY coding_system, coding_code
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| MetricSummary {
            coding_system: r.coding_system,
            coding_code: r.coding_code,
            count: r.count,
            first_effective_start: r.first_effective_start,
            last_effective_start: r.last_effective_start,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queries::test_support::{seed_interval_observations, IntervalObsSpec};
    use time::macros::datetime;

    #[tokio::test]
    async fn empty_vec_when_no_observations() {
        let (pool, _) = seed_interval_observations(&[]).await;
        let metrics = counts_per_code(&pool).await.expect("query");
        assert!(metrics.is_empty());
    }

    #[tokio::test]
    async fn groups_by_system_and_code_with_count_and_span() {
        let (pool, _) = seed_interval_observations(&[
            IntervalObsSpec {
                coding_system: "http://loinc.org",
                coding_code: "29463-7",
                effective_start: datetime!(2026-01-01 12:00:00 UTC),
                effective_end: datetime!(2026-01-01 12:05:00 UTC),
                value_quantity: 72.5,
            },
            IntervalObsSpec {
                coding_system: "http://loinc.org",
                coding_code: "29463-7",
                effective_start: datetime!(2026-03-01 12:00:00 UTC),
                effective_end: datetime!(2026-03-01 12:05:00 UTC),
                value_quantity: 72.0,
            },
            IntervalObsSpec {
                coding_system: "https://chartpds.fhwang.net/coding/aasm/sleep-stage",
                coding_code: "aasm-sleep-stage",
                effective_start: datetime!(2026-02-01 00:00:00 UTC),
                effective_end: datetime!(2026-02-01 00:05:00 UTC),
                value_quantity: 3.0,
            },
        ])
        .await;

        let metrics = counts_per_code(&pool).await.expect("query");
        assert_eq!(metrics.len(), 2);

        // Ordered by system then code. Lexically "http://loinc.org" sorts
        // before "https://chartpds..." (char 5 ':' (0x3A) < 's' (0x73)), so
        // the loinc weight comes first and the aasm sleep stage second.
        let weight = &metrics[0];
        assert_eq!(weight.coding_system, "http://loinc.org");
        assert_eq!(weight.coding_code, "29463-7");
        assert_eq!(weight.count, 2);
        assert_eq!(weight.first_effective_start, datetime!(2026-01-01 12:00:00 UTC));
        assert_eq!(weight.last_effective_start, datetime!(2026-03-01 12:00:00 UTC));

        assert_eq!(metrics[1].coding_code, "aasm-sleep-stage");
        assert_eq!(
            metrics[1].coding_system,
            "https://chartpds.fhwang.net/coding/aasm/sleep-stage"
        );
        assert_eq!(metrics[1].count, 1);
    }
}
```

- [ ] **Step 2: Update the re-export in `queries/mod.rs`**

Change:
```rust
pub use counts_per_code::{counts_per_code, CodeCount};
```
to:
```rust
pub use counts_per_code::{counts_per_code, MetricSummary};
```

- [ ] **Step 3: Regenerate the sqlx cache**

Run: `just prepare-sql`
Expected: updates `.sqlx/` JSON for the changed `counts_per_code` query; no error.

- [ ] **Step 4: Run the core test to verify it passes**

Run: `cargo test -p chartpds-core counts_per_code`
Expected: PASS (`empty_vec_when_no_observations`, `groups_by_system_and_code_with_count_and_span`).

- [ ] **Step 5: Update the `observation_counts` tool description in `server.rs`**

Replace the `#[tool(description = …)]` above `async fn observation_counts` (currently "Count observations grouped by LOINC code…") with:

```rust
    #[tool(
        description = "Discover which codings are present in the store. Returns [{coding_system, coding_code, count, first_effective_start, last_effective_start}] grouped by (system, code), ordered by system then code. Feed {coding_system, coding_code} into the history/aggregator tools. Empty array means an empty store; last_effective_start is the per-coding freshness signal."
    )]
```

The method body is unchanged (it serializes whatever `counts_per_code` returns).

- [ ] **Step 6: Update the server test `observation_counts_returns_one_entry`**

The exact-match assertion is now wrong. Replace the assertion block (from `let text = match …` through the `assert_eq!`) with a parsed-field check that does not depend on timestamp formatting beyond presence:

```rust
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let v: serde_json::Value = serde_json::from_str(text).expect("valid json");
        let arr = v.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["coding_system"], "http://loinc.org");
        assert_eq!(arr[0]["coding_code"], "29463-7");
        assert_eq!(arr[0]["count"], 1);
        assert_eq!(arr[0]["first_effective_start"], "2026-01-01T12:00:00Z");
        assert_eq!(arr[0]["last_effective_start"], "2026-01-01T12:00:00Z");
```

(The seeded weight in `fresh_server_with_one_weight` has `effective_start: datetime!(2026-01-01 12:00:00 UTC)`, and the `#[serde(with = "time::serde::rfc3339")]` attribute guarantees the `2026-01-01T12:00:00Z` form.)

- [ ] **Step 7: Run the server test to verify it passes**

Run: `cargo test -p chartpds-mcp observation_counts_returns_one_entry`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/chartpds-core/src/queries/counts_per_code.rs \
        crates/chartpds-core/src/queries/mod.rs \
        crates/chartpds-mcp/src/server.rs \
        .sqlx
git commit -m "P2 #6: enrich observation_counts with system + date span

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: #6b — Minted-coding catalog (core)

**Files:**
- Modify: `crates/chartpds-core/src/clinical/aasm.rs`
- Create: `crates/chartpds-core/src/clinical/catalog.rs`
- Modify: `crates/chartpds-core/src/clinical/mod.rs`

**Interfaces:**
- Consumes: `AasmSleepStage`, `AASM_SLEEP_STAGE_CODE`, `AASM_SLEEP_STAGE_SYSTEM` (existing `clinical` re-exports).
- Produces: `chartpds_core::clinical::{minted_coding_definitions, CodingDefinition, CodingValue}`. `minted_coding_definitions() -> Vec<CodingDefinition>`. `AasmSleepStage::{ALL, as_str(), label(), discriminant()}`.

- [ ] **Step 1: Add `ALL`, `as_str`, `label`, `discriminant` to `AasmSleepStage` and refactor `Display`**

In `crates/chartpds-core/src/clinical/aasm.rs`, add an `impl` block (place it right after the `enum AasmSleepStage { … }` definition, before the `impl Display`):

```rust
impl AasmSleepStage {
    /// All stages, ascending by discriminant. Drives the minted-coding
    /// catalog so its value list cannot drift from the encoder.
    pub const ALL: [AasmSleepStage; 5] = [
        Self::Wake,
        Self::N1,
        Self::N2,
        Self::N3,
        Self::Rem,
    ];

    /// Stable lowercase token, identical to the stored `value_string`.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Wake => "wake",
            Self::N1 => "n1",
            Self::N2 => "n2",
            Self::N3 => "n3",
            Self::Rem => "rem",
        }
    }

    /// Human-facing label for catalog/display use.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Wake => "awake",
            Self::N1 => "light sleep (N1, transition)",
            Self::N2 => "light sleep (N2)",
            Self::N3 => "deep / slow-wave sleep (N3)",
            Self::Rem => "REM",
        }
    }

    /// Numeric discriminant as stored in `value_quantity`.
    #[must_use]
    pub fn discriminant(&self) -> u8 {
        *self as u8
    }
}
```

Then change the `Display` impl body to reuse `as_str`:

```rust
impl std::fmt::Display for AasmSleepStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
```

- [ ] **Step 2: Create `clinical/catalog.rs`**

```rust
//! Static catalog describing the codings ChartPDS itself mints.
//!
//! Standard codings (LOINC, …) are self-describing to a client that knows the
//! vocabulary and are deliberately omitted. The sleep-stage entry is derived
//! from [`AasmSleepStage`] so the catalog cannot drift from the encoder.

use crate::clinical::{AasmSleepStage, AASM_SLEEP_STAGE_CODE, AASM_SLEEP_STAGE_SYSTEM};

/// One allowed value of a minted coding's value vocabulary.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct CodingValue {
    /// Numeric value stored in `value_quantity`.
    pub value_quantity: f64,
    /// String value stored in `value_string`.
    pub value_string: &'static str,
    /// Human-facing label for this value.
    pub label: &'static str,
}

/// A self-description of one ChartPDS-minted coding.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct CodingDefinition {
    /// FHIR coding system URI.
    pub coding_system: &'static str,
    /// Coding code within the system.
    pub coding_code: &'static str,
    /// What the coding measures.
    pub description: &'static str,
    /// How to read `value_quantity`.
    pub value_quantity_meaning: &'static str,
    /// How to read `value_string`.
    pub value_string_meaning: &'static str,
    /// The value vocabulary.
    pub values: Vec<CodingValue>,
    /// Practical hints for driving the aggregator/history tools.
    pub hints: Vec<&'static str>,
}

/// All ChartPDS-minted coding definitions (LOINC and other standard codings
/// are excluded as self-describing).
#[must_use]
pub fn minted_coding_definitions() -> Vec<CodingDefinition> {
    vec![sleep_stage_definition()]
}

fn sleep_stage_definition() -> CodingDefinition {
    let values = AasmSleepStage::ALL
        .iter()
        .map(|s| CodingValue {
            value_quantity: f64::from(s.discriminant()),
            value_string: s.as_str(),
            label: s.label(),
        })
        .collect();
    CodingDefinition {
        coding_system: AASM_SLEEP_STAGE_SYSTEM,
        coding_code: AASM_SLEEP_STAGE_CODE,
        description: "Per-epoch (5-minute) AASM sleep stage. One observation per 5-min epoch; effective_start/end bound the epoch.",
        value_quantity_meaning: "AASM stage discriminant. Monotonic: 0 = wake, >=1 = asleep.",
        value_string_meaning: "Stage name matching the discriminant.",
        values,
        hints: vec!["For 'asleep' totals use value_range {min: 1, max: 4}; wake is 0."],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_exactly_the_sleep_stage_coding() {
        let defs = minted_coding_definitions();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].coding_code, AASM_SLEEP_STAGE_CODE);
        assert_eq!(defs[0].coding_system, AASM_SLEEP_STAGE_SYSTEM);
    }

    #[test]
    fn sleep_stage_values_match_enum() {
        let def = sleep_stage_definition();
        assert_eq!(def.values.len(), AasmSleepStage::ALL.len());
        for (v, stage) in def.values.iter().zip(AasmSleepStage::ALL.iter()) {
            assert_eq!(v.value_quantity, f64::from(stage.discriminant()));
            assert_eq!(v.value_string, stage.as_str());
            assert_eq!(v.label, stage.label());
        }
    }
}
```

- [ ] **Step 3: Declare and re-export the catalog in `clinical/mod.rs`**

Add `mod catalog;` alongside the other `mod` lines, and add a re-export:

```rust
pub use catalog::{minted_coding_definitions, CodingDefinition, CodingValue};
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p chartpds-core clinical::`
Expected: PASS, including `sleep_stage_values_match_enum`, `returns_exactly_the_sleep_stage_coding`, and the existing `aasm` tests (Display still maps `wake/n1/n2/n3/rem`).

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/clinical/
git commit -m "P2 #6b: minted-coding catalog derived from AasmSleepStage

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: #6b — `describe_codings` MCP tool

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs`

**Interfaces:**
- Consumes: `chartpds_core::clinical::minted_coding_definitions`.
- Produces: the `describe_codings` MCP tool (no args).

- [ ] **Step 1: Add the args struct**

Near the other `*Args` structs in `server.rs`, add:

```rust
/// Arguments for the `describe_codings` tool (none).
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct DescribeCodingsArgs {}
```

- [ ] **Step 2: Add the tool method inside the `#[tool_router] impl ChartPdsServer` block**

Place it after `observation_counts` (before `list_problems`):

```rust
    #[tool(
        description = "Describe the non-standard codings ChartPDS mints, including value-encoding semantics a client cannot infer. Standard codings (LOINC) are omitted as self-describing. Returns [{coding_system, coding_code, description, value_quantity_meaning, value_string_meaning, values:[{value_quantity, value_string, label}], hints}]."
    )]
    async fn describe_codings(
        &self,
        Parameters(_args): Parameters<DescribeCodingsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let defs = chartpds_core::clinical::minted_coding_definitions();
        let json = serde_json::to_string(&defs)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }
```

- [ ] **Step 3: Add a server test**

In the `#[cfg(test)] mod tests` block, add:

```rust
    #[tokio::test]
    async fn describe_codings_returns_sleep_stage_catalog() {
        let server = fresh_server_with_empty_db().await;
        let result = server
            .describe_codings(Parameters(DescribeCodingsArgs {}))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let v: serde_json::Value = serde_json::from_str(text).expect("valid json");
        let arr = v.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["coding_code"], "aasm-sleep-stage");
        assert_eq!(
            arr[0]["coding_system"],
            "https://chartpds.fhwang.net/coding/aasm/sleep-stage"
        );
        assert_eq!(arr[0]["values"].as_array().expect("values array").len(), 5);
        // Spot-check the encoding a client needs.
        assert_eq!(arr[0]["values"][0]["value_quantity"], 0.0);
        assert_eq!(arr[0]["values"][0]["value_string"], "wake");
        assert_eq!(arr[0]["values"][4]["value_string"], "rem");
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p chartpds-mcp describe_codings_returns_sleep_stage_catalog`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-mcp/src/server.rs
git commit -m "P2 #6b: add describe_codings MCP tool

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: #7 — `observation_history` core query

**Files:**
- Create: `crates/chartpds-core/src/queries/observation_history.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs`

**Interfaces:**
- Consumes: `crate::index::Observation`.
- Produces: `chartpds_core::queries::{observation_history, CodingKey}`. `CodingKey<'a> { coding_system: &'a str, coding_code: &'a str }`. `observation_history(&SqlitePool, &[CodingKey<'_>], Option<OffsetDateTime>, Option<OffsetDateTime>) -> Result<Vec<Observation>, sqlx::Error>`.

- [ ] **Step 1: Create `observation_history.rs`**

```rust
//! Multi-coding observation history with optional open-ended bounds.

use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::index::Observation;

/// A `(system, code)` selector for [`observation_history`].
#[derive(Debug, Clone, Copy)]
pub struct CodingKey<'a> {
    /// FHIR coding system URI.
    pub coding_system: &'a str,
    /// Coding code within the system.
    pub coding_code: &'a str,
}

/// Fetch observations for any of `codings` whose `effective_start` falls within
/// the optional half-open bounds `[since, until)`. Either bound may be `None`
/// (open-ended); both `None` reads full history. Matching is by
/// `(coding_system, coding_code)`. Results are ordered by
/// `(coding_system, coding_code, effective_start)`.
///
/// An empty `codings` slice returns an empty vec without touching the database.
///
/// # Errors
///
/// Returns `sqlx::Error` if any underlying query fails.
pub async fn observation_history(
    pool: &SqlitePool,
    codings: &[CodingKey<'_>],
    since: Option<OffsetDateTime>,
    until: Option<OffsetDateTime>,
) -> Result<Vec<Observation>, sqlx::Error> {
    let mut out = Vec::new();

    for coding in codings {
        let system = coding.coding_system;
        let code = coding.coding_code;
        let rows = sqlx::query!(
            r#"
            SELECT id AS "id!: i64",
                   source_document_id AS "source_document_id!: i64",
                   coding_system, coding_code, coding_display,
                   effective_start AS "effective_start: OffsetDateTime",
                   effective_end AS "effective_end?: OffsetDateTime",
                   value_quantity, value_string, value_unit
            FROM observations
            WHERE coding_system = ?
              AND coding_code = ?
              AND (? IS NULL OR effective_start >= ?)
              AND (? IS NULL OR effective_start <  ?)
            ORDER BY effective_start
            "#,
            system,
            code,
            since,
            since,
            until,
            until,
        )
        .fetch_all(pool)
        .await?;

        out.extend(rows.into_iter().map(|r| Observation {
            id: r.id,
            source_document_id: r.source_document_id,
            coding_system: r.coding_system,
            coding_code: r.coding_code,
            coding_display: r.coding_display,
            effective_start: r.effective_start,
            effective_end: r.effective_end,
            value_quantity: r.value_quantity,
            value_string: r.value_string,
            value_unit: r.value_unit,
        }));
    }

    out.sort_by(|a, b| {
        a.coding_system
            .cmp(&b.coding_system)
            .then_with(|| a.coding_code.cmp(&b.coding_code))
            .then_with(|| a.effective_start.cmp(&b.effective_start))
    });

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queries::test_support::{seed_interval_observations, IntervalObsSpec};
    use time::macros::datetime;

    const LOINC: &str = "http://loinc.org";
    const AASM: &str = "https://chartpds.fhwang.net/coding/aasm/sleep-stage";

    async fn seed() -> SqlitePool {
        let (pool, _) = seed_interval_observations(&[
            IntervalObsSpec {
                coding_system: LOINC,
                coding_code: "8867-4",
                effective_start: datetime!(2026-01-01 00:00:00 UTC),
                effective_end: datetime!(2026-01-01 00:01:00 UTC),
                value_quantity: 60.0,
            },
            IntervalObsSpec {
                coding_system: LOINC,
                coding_code: "8867-4",
                effective_start: datetime!(2026-02-01 00:00:00 UTC),
                effective_end: datetime!(2026-02-01 00:01:00 UTC),
                value_quantity: 65.0,
            },
            IntervalObsSpec {
                coding_system: AASM,
                coding_code: "aasm-sleep-stage",
                effective_start: datetime!(2026-01-15 00:00:00 UTC),
                effective_end: datetime!(2026-01-15 00:05:00 UTC),
                value_quantity: 3.0,
            },
        ])
        .await;
        pool
    }

    #[tokio::test]
    async fn empty_codings_returns_empty() {
        let pool = seed().await;
        let rows = observation_history(&pool, &[], None, None).await.expect("query");
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn multi_coding_full_history_ordered_by_system_code_time() {
        let pool = seed().await;
        let rows = observation_history(
            &pool,
            &[
                CodingKey { coding_system: LOINC, coding_code: "8867-4" },
                CodingKey { coding_system: AASM, coding_code: "aasm-sleep-stage" },
            ],
            None,
            None,
        )
        .await
        .expect("query");

        assert_eq!(rows.len(), 3);
        // http://loinc.org sorts before https://chartpds...
        assert_eq!(rows[0].coding_system, LOINC);
        assert_eq!(rows[0].effective_start, datetime!(2026-01-01 00:00:00 UTC));
        assert_eq!(rows[1].coding_system, LOINC);
        assert_eq!(rows[1].effective_start, datetime!(2026-02-01 00:00:00 UTC));
        assert_eq!(rows[2].coding_system, AASM);
    }

    #[tokio::test]
    async fn since_only_is_open_ended_upper() {
        let pool = seed().await;
        let rows = observation_history(
            &pool,
            &[CodingKey { coding_system: LOINC, coding_code: "8867-4" }],
            Some(datetime!(2026-01-15 00:00:00 UTC)),
            None,
        )
        .await
        .expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].effective_start, datetime!(2026-02-01 00:00:00 UTC));
    }

    #[tokio::test]
    async fn until_only_is_open_ended_lower_and_exclusive() {
        let pool = seed().await;
        let rows = observation_history(
            &pool,
            &[CodingKey { coding_system: LOINC, coding_code: "8867-4" }],
            None,
            Some(datetime!(2026-02-01 00:00:00 UTC)),
        )
        .await
        .expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].effective_start, datetime!(2026-01-01 00:00:00 UTC));
    }
}
```

- [ ] **Step 2: Wire it into `queries/mod.rs`; remove `in_range`**

Replace the `in_range` module line and re-export. Change:
```rust
mod in_range;
```
to:
```rust
mod observation_history;
```
and change:
```rust
pub use in_range::in_range;
```
to:
```rust
pub use observation_history::{observation_history, CodingKey};
```

- [ ] **Step 3: Delete the old query file**

Run: `git rm crates/chartpds-core/src/queries/in_range.rs`
Expected: file removed. (The server still references `queries::in_range` until Task 5 — that's fine; this task is committed together with Task 5's compile fix below. To keep this task independently green, perform Steps 1–6 of Task 5 BEFORE building. See note.)

> **Sequencing note:** removing `in_range` breaks `server.rs` compilation until the tool is swapped. To keep a green commit, **do Task 5 immediately after Step 1–2 here, in the same commit.** Steps 3–5 below are the core-side verification once the server is updated.

- [ ] **Step 4: Regenerate the sqlx cache**

Run: `just prepare-sql`
Expected: adds `.sqlx/` JSON for the new `observation_history` query, drops the old `in_range` entry; no error.

- [ ] **Step 5: Run the core tests**

Run: `cargo test -p chartpds-core observation_history`
Expected: PASS (4 tests).

- [ ] **Step 6: (commit happens at the end of Task 5)**

---

## Task 5: #7 — Swap `observations_in_range` → `get_observation_history` tool

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs`

**Interfaces:**
- Consumes: `chartpds_core::queries::{observation_history, CodingKey}`, existing `Coding` arg struct.
- Produces: the `get_observation_history` MCP tool; removes `observations_in_range`.

- [ ] **Step 1: Replace the `ObservationsInRangeArgs` struct with `GetObservationHistoryArgs`**

Replace:
```rust
/// Arguments for the `observations_in_range` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct ObservationsInRangeArgs {
    /// LOINC code to filter on.
    pub(crate) code: String,
    /// Inclusive start of the time window (RFC 3339).
    pub(crate) start: String,
    /// Exclusive end of the time window (RFC 3339).
    pub(crate) end: String,
}
```
with:
```rust
/// Arguments for the `get_observation_history` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct GetObservationHistoryArgs {
    /// One or more codings to read. Each is `{system, code}`.
    pub(crate) codings: Vec<Coding>,
    /// Optional inclusive lower bound on `effective_start` (RFC 3339).
    pub(crate) since: Option<String>,
    /// Optional exclusive upper bound on `effective_start` (RFC 3339).
    pub(crate) until: Option<String>,
}
```

(`Coding` is already defined later in the file; struct order does not matter in Rust.)

- [ ] **Step 2: Replace the `observations_in_range` tool method with `get_observation_history`**

Replace the whole `#[tool(...)] async fn observations_in_range(...) { ... }` block with:

```rust
    #[tool(
        description = "Read observation history across one or more codings, with optional open-ended bounds. Args: codings [{system, code}], since? (RFC 3339, inclusive), until? (RFC 3339, exclusive); omit either bound for open-ended, omit both for full history. Returns a flat JSON array of observations ordered by (coding_system, coding_code, effective_start)."
    )]
    async fn get_observation_history(
        &self,
        Parameters(args): Parameters<GetObservationHistoryArgs>,
    ) -> Result<CallToolResult, McpError> {
        let since = match args.since.as_deref() {
            Some(s) => Some(
                time::OffsetDateTime::parse(s, &Rfc3339)
                    .map_err(|err| McpError::invalid_params(format!("invalid since: {err}"), None))?,
            ),
            None => None,
        };
        let until = match args.until.as_deref() {
            Some(s) => Some(
                time::OffsetDateTime::parse(s, &Rfc3339)
                    .map_err(|err| McpError::invalid_params(format!("invalid until: {err}"), None))?,
            ),
            None => None,
        };

        let codings: Vec<chartpds_core::queries::CodingKey<'_>> = args
            .codings
            .iter()
            .map(|c| chartpds_core::queries::CodingKey {
                coding_system: &c.system,
                coding_code: &c.code,
            })
            .collect();

        let rows = chartpds_core::queries::observation_history(&self.pool, &codings, since, until)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&rows)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }
```

- [ ] **Step 3: Replace the two `observations_in_range` server tests**

Delete `observations_in_range_returns_match_in_window` and `observations_in_range_returns_empty_outside_window`, and add:

```rust
    #[tokio::test]
    async fn get_observation_history_returns_match_for_coding() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .get_observation_history(Parameters(GetObservationHistoryArgs {
                codings: vec![Coding {
                    system: "http://loinc.org".to_owned(),
                    code: "29463-7".to_owned(),
                }],
                since: None,
                until: None,
            }))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let arr: serde_json::Value = serde_json::from_str(text).expect("valid json");
        let arr = arr.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["coding_code"], "29463-7");
    }

    #[tokio::test]
    async fn get_observation_history_empty_when_coding_absent() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .get_observation_history(Parameters(GetObservationHistoryArgs {
                codings: vec![Coding {
                    system: "http://loinc.org".to_owned(),
                    code: "no-such-code".to_owned(),
                }],
                since: None,
                until: None,
            }))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        assert_eq!(text, "[]");
    }
```

- [ ] **Step 4: Build to verify the `in_range` removal is fully resolved**

Run: `cargo build`
Expected: compiles cleanly (no references to `queries::in_range` or `observations_in_range` remain).

- [ ] **Step 5: Run the affected tests**

Run: `cargo test -p chartpds-mcp get_observation_history`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit Tasks 4 + 5 together**

```bash
git add crates/chartpds-core/src/queries/ \
        crates/chartpds-mcp/src/server.rs \
        .sqlx
git commit -m "P2 #7: replace observations_in_range with get_observation_history

Multi-coding, optional open-ended bounds, system-aware matching; remove the
single-code in_range query.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: #9 — WASO parser function (core)

**Files:**
- Modify: `crates/chartpds-core/src/sources/oura/parser.rs`

**Interfaces:**
- Consumes: `OuraSleepSession`, `parse_sleep_epochs`, `AasmSleepStage`, `EPOCH_SECONDS`.
- Produces: `ParsedWaso { effective_start, effective_end, minutes }` and `wake_after_sleep_onset(&OuraSleepSession) -> sources::Result<Option<ParsedWaso>>` (both `pub` within the oura module).

- [ ] **Step 1: Write the failing tests**

In `parser.rs`'s `#[cfg(test)] mod tests`, add (mirror the `session(...)` helper style already used by the nightly tests — if a helper builds an `OuraSleepSession`, reuse it; otherwise construct inline as below):

```rust
    fn waso_session(session_type: &str, phases: &str) -> OuraSleepSession {
        OuraSleepSession {
            id: "s1".to_owned(),
            day: "2026-01-15".to_owned(),
            bedtime_start: "2026-01-14T22:00:00Z".to_owned(),
            bedtime_end: "2026-01-15T06:00:00Z".to_owned(),
            session_type: session_type.to_owned(),
            sleep_phase_5_min: phases.to_owned(),
            total_sleep_duration: None,
            rem_sleep_duration: None,
            deep_sleep_duration: None,
            light_sleep_duration: None,
        }
    }

    #[test]
    fn waso_counts_only_interior_wake() {
        // W W N2 N2 W N1 REM -> onset idx 2, final idx 6, one interior wake.
        let s = waso_session("long_sleep", "4422413");
        let waso = wake_after_sleep_onset(&s).expect("ok").expect("some");
        assert_eq!(waso.minutes, 5.0);
        assert_eq!(waso.effective_start, datetime!(2026-01-14 22:00:00 UTC));
        assert_eq!(waso.effective_end, datetime!(2026-01-15 06:00:00 UTC));
    }

    #[test]
    fn waso_zero_for_unbroken_night() {
        // N3 N3 REM -> onset idx 0, final idx 2, no interior wake.
        let s = waso_session("long_sleep", "113");
        let waso = wake_after_sleep_onset(&s).expect("ok").expect("some");
        assert_eq!(waso.minutes, 0.0);
    }

    #[test]
    fn waso_excludes_leading_and_trailing_wake() {
        // W W N2 W N2 W W -> onset idx 2, final idx 4, one interior wake (idx 3).
        let s = waso_session("long_sleep", "4424244");
        let waso = wake_after_sleep_onset(&s).expect("ok").expect("some");
        assert_eq!(waso.minutes, 5.0);
    }

    #[test]
    fn waso_none_when_all_wake() {
        let s = waso_session("long_sleep", "444");
        assert!(wake_after_sleep_onset(&s).expect("ok").is_none());
    }

    #[test]
    fn waso_none_when_empty() {
        let s = waso_session("long_sleep", "");
        assert!(wake_after_sleep_onset(&s).expect("ok").is_none());
    }

    #[test]
    fn waso_none_for_nap() {
        let s = waso_session("late_nap", "4224");
        assert!(wake_after_sleep_onset(&s).expect("ok").is_none());
    }
```

> Before writing, check the existing nightly tests for an `OuraSleepSession` constructor helper (e.g. a `session(...)` fn). If one exists with the right fields, call it instead of duplicating `waso_session`. The `OuraSleepSession` fields are: `id, day, bedtime_start, bedtime_end, session_type, sleep_phase_5_min, total_sleep_duration, rem_sleep_duration, deep_sleep_duration, light_sleep_duration` (see `oura/api.rs`).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p chartpds-core waso`
Expected: FAIL — `wake_after_sleep_onset` / `ParsedWaso` not found.

- [ ] **Step 3: Implement `ParsedWaso` and `wake_after_sleep_onset`**

Add to `parser.rs` (after `nightly_sleep_duration`):

```rust
/// A derived Wake-After-Sleep-Onset summary for one night.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedWaso {
    /// Session start (observation `effective_start`).
    pub effective_start: OffsetDateTime,
    /// Session end (observation `effective_end`).
    pub effective_end: OffsetDateTime,
    /// Wake minutes between sleep onset and final awakening.
    pub minutes: f64,
}

/// Derive Wake-After-Sleep-Onset (WASO) for a session.
///
/// WASO is the wake time between sleep onset (first non-wake epoch) and final
/// awakening (last non-wake epoch); pre-onset latency and post-waking time are
/// excluded by construction. Returns `Some(0.0)` for an unbroken night and
/// `None` when the session never reaches sleep (or is not a `long_sleep`).
///
/// # Errors
///
/// Returns [`sources::Error::Parse`] if `bedtime_start`/`bedtime_end` is not
/// valid RFC 3339, or if `sleep_phase_5_min` contains an unknown stage char.
pub fn wake_after_sleep_onset(
    session: &OuraSleepSession,
) -> sources::Result<Option<ParsedWaso>> {
    if session.session_type != "long_sleep" {
        return Ok(None);
    }

    let epochs = parse_sleep_epochs(&session.bedtime_start, &session.sleep_phase_5_min)?;

    let Some(onset) = epochs.iter().position(|e| e.stage != AasmSleepStage::Wake) else {
        return Ok(None);
    };
    let final_wake = epochs
        .iter()
        .rposition(|e| e.stage != AasmSleepStage::Wake)
        .expect("onset exists, so a last non-wake epoch exists");

    let wake_epochs = epochs[onset..=final_wake]
        .iter()
        .filter(|e| e.stage == AasmSleepStage::Wake)
        .count();

    #[allow(
        clippy::cast_precision_loss,
        reason = "epoch count and EPOCH_SECONDS for one night fit f64 without loss"
    )]
    let minutes = wake_epochs as f64 * EPOCH_SECONDS as f64 / 60.0;

    let effective_start =
        OffsetDateTime::parse(&session.bedtime_start, &Rfc3339).map_err(|err| {
            sources::Error::Parse {
                reason: format!("invalid bedtime_start {:?}: {err}", session.bedtime_start),
            }
        })?;
    let effective_end = OffsetDateTime::parse(&session.bedtime_end, &Rfc3339).map_err(|err| {
        sources::Error::Parse {
            reason: format!("invalid bedtime_end {:?}: {err}", session.bedtime_end),
        }
    })?;

    Ok(Some(ParsedWaso {
        effective_start,
        effective_end,
        minutes,
    }))
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p chartpds-core waso`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/sources/oura/parser.rs
git commit -m "P2 #9: derive WASO from Oura sleep epochs

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: #9 — Emit the WASO observation from Oura storage

**Files:**
- Modify: `crates/chartpds-core/src/clinical/coding.rs`
- Modify: `crates/chartpds-core/src/clinical/mod.rs`
- Modify: `crates/chartpds-core/src/sources/oura/storage.rs`

**Interfaces:**
- Consumes: `wake_after_sleep_onset`, `index::insert_observation`, `SYSTEM_LOINC`, new `LOINC_WASO`.
- Produces: a `103215-0` observation per `long_sleep` night, written by both live ingest and archive replay (shared `index_sleep_session`).

- [ ] **Step 1: Add the `LOINC_WASO` constant**

In `crates/chartpds-core/src/clinical/coding.rs`, next to `LOINC_SLEEP_DURATION`:

```rust
/// LOINC code for Wake-After-Sleep-Onset (WASO), in minutes.
pub const LOINC_WASO: &str = "103215-0";
```

- [ ] **Step 2: Re-export it from `clinical/mod.rs`**

Add `LOINC_WASO` to the `pub use coding::{…}` list (keep the existing names):

```rust
pub use coding::{
    fhir_system_for_oid, LOINC_SLEEP_DURATION, LOINC_WASO, SYSTEM_ICD10, SYSTEM_LOINC,
    SYSTEM_RXNORM, SYSTEM_SNOMED,
};
```

- [ ] **Step 3: Import `LOINC_WASO` in `storage.rs`**

Change the `use crate::clinical::{…}` line to include `LOINC_WASO`:

```rust
use crate::clinical::{
    AASM_SLEEP_STAGE_CODE, AASM_SLEEP_STAGE_SYSTEM, LOINC_SLEEP_DURATION, LOINC_WASO, SYSTEM_LOINC,
};
```

- [ ] **Step 4: Emit the WASO observation in `index_sleep_session`**

Immediately after the `if let Some(nightly) = parser::nightly_sleep_duration(session)? { … }` block, add:

```rust
    if let Some(waso) = parser::wake_after_sleep_onset(session)? {
        index::insert_observation(
            pool,
            index::InsertObservationParams {
                source_document_id,
                coding_system: SYSTEM_LOINC,
                coding_code: LOINC_WASO,
                coding_display: Some("Wake after sleep onset"),
                effective_start: waso.effective_start,
                effective_end: Some(waso.effective_end),
                value_quantity: Some(waso.minutes),
                value_string: None,
                value_unit: Some("min"),
            },
        )
        .await?;
    }
```

- [ ] **Step 5: Add a storage test asserting WASO is emitted**

In `storage.rs`'s `#[cfg(test)] mod tests`, model it on the existing `ingest_session_archives_and_inserts_observations` test (which seeds a session and queries observations via `list_observations_by_source_document`). Add:

```rust
    #[tokio::test]
    async fn ingest_emits_waso_observation() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);

        // W W N2 N2 W N1 REM -> one interior wake epoch -> WASO 5 min.
        let session = OuraSleepSession {
            id: "s1".to_owned(),
            day: "2026-01-15".to_owned(),
            bedtime_start: "2026-01-14T22:00:00Z".to_owned(),
            bedtime_end: "2026-01-15T06:00:00Z".to_owned(),
            session_type: "long_sleep".to_owned(),
            sleep_phase_5_min: "4422413".to_owned(),
            total_sleep_duration: Some(28800),
            rem_sleep_duration: None,
            deep_sleep_duration: None,
            light_sleep_duration: None,
        };
        let raw = serde_json::json!({ "data": [ { "id": "s1" } ] });

        let doc_id = ingest_session(&archive, &pool, &session, &raw)
            .await
            .expect("ingest");

        let obs = list_observations_by_source_document(&pool, doc_id)
            .await
            .expect("list");
        let waso: Vec<_> = obs.iter().filter(|o| o.coding_code == "103215-0").collect();
        assert_eq!(waso.len(), 1);
        assert_eq!(waso[0].coding_system, "http://loinc.org");
        assert_eq!(waso[0].value_quantity, Some(5.0));
        assert_eq!(waso[0].value_unit.as_deref(), Some("min"));
    }
```

> Check the existing storage tests for the exact imports/helpers in scope (`Archive`, `InMemory`, `Arc`, `open_pool`, `list_observations_by_source_document`, `ingest_session`, `OuraSleepSession`). Reuse them; the `use super::*;` plus the existing `use` lines at the top of the test module should already cover these (see lines ~196–200). Add any missing import (e.g. `std::sync::Arc`) as the neighbouring tests do.

- [ ] **Step 6: Run the test to verify it passes**

Run: `cargo test -p chartpds-core ingest_emits_waso_observation`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/chartpds-core/src/clinical/coding.rs \
        crates/chartpds-core/src/clinical/mod.rs \
        crates/chartpds-core/src/sources/oura/storage.rs
git commit -m "P2 #9: emit per-night WASO (LOINC 103215-0) from Oura storage

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Docs + full verification

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update the MCP server tool list in `CLAUDE.md`**

In the "MCP server" section, the bullet list of tools: replace the `observations_in_range` bullet with `get_observation_history`, update the `observation_counts` bullet, and add a `describe_codings` bullet. Use:

- `get_observation_history` — observations across one or more codings, with optional open-ended `since`/`until` bounds (replaces `observations_in_range`)
- `observation_counts` — discover codings present in the store: `{coding_system, coding_code, count, first_effective_start, last_effective_start}` per `(system, code)`
- `describe_codings` — value-encoding semantics for the codings ChartPDS mints (non-standard only; LOINC omitted as self-describing)

Update the "serves 12 tools" count to the new total (was 12; −1 `observations_in_range`, +2 `get_observation_history`/`describe_codings` = **13**, and `observation_counts` is unchanged in count). Verify by counting the `#[tool(...)]` methods in `server.rs`.

- [ ] **Step 2: Update the "Queries" section in `CLAUDE.md`**

In the `queries::` paragraph, replace the mention of `in_range` with `observation_history` and note it takes multiple `{system, code}` codings with optional bounds. Add a one-line note that `counts_per_code` now returns per-`(system, code)` summaries with a date span.

- [ ] **Step 3: Update the Oura section in `CLAUDE.md`**

Add a sentence noting the Oura adapter emits, per `long_sleep` night, a nightly total-sleep observation (LOINC `93832-4`) and a WASO observation (LOINC `103215-0`, minutes) alongside the per-epoch `aasm-sleep-stage` observations.

- [ ] **Step 4: Run the full check gate**

Run: `just check`
Expected: PASS — `fmt-check`, `lint` (`-D warnings`, so every new `pub` item must have a doc comment), `typecheck`, `test`, `cargo deny`, `cargo machete`, and `cargo sqlx prepare --check` all green.

If `cargo sqlx prepare --check` fails: run `just prepare-sql`, `git add .sqlx`, and amend the relevant query commit (or add a follow-up commit).

- [ ] **Step 5: Commit the docs**

```bash
git add CLAUDE.md
git commit -m "P2: document discovery, history, describe_codings, and WASO tools

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: Note the backfill step for the operator**

WASO and any other newly-derived fields land for *new* syncs automatically. To populate WASO for already-archived Oura blobs, the operator runs the `rebuild_index` MCP tool once after deploy (storage re-derives on replay; no re-pull from Oura). State this in the PR description — no code action needed.

---

## Self-Review

**Spec coverage:**
- #6 (enrich `observation_counts`) → Task 1. ✓
- #6b (`describe_codings`, minted-only, enum-derived, static) → Tasks 2–3. ✓
- #7 (`get_observation_history`, multi-coding, optional bounds, replace `observations_in_range`, flat array) → Tasks 4–5. ✓
- #9 (epoch-derived WASO, `long_sleep` only, 0-vs-None edges, LOINC `103215-0`, no schema change, backfill via rebuild) → Tasks 6–7. ✓
- Dropped `list_documents` → not built (correct). ✓
- Docs/CLAUDE.md → Task 8. ✓
- `oura/parser.rs` split watch-item → noted in spec; not acted on (correct, 3 functions). ✓

**Placeholder scan:** none — all steps carry concrete code/commands. The one judgment step (reusing an existing `OuraSleepSession` test helper if present) gives the full field list as a fallback.

**Type consistency:**
- `MetricSummary` (Task 1) used consistently in struct + re-export.
- `CodingKey { coding_system, coding_code }` defined in Task 4, consumed identically in Task 5's server mapping.
- `ParsedWaso { effective_start, effective_end, minutes }` defined in Task 6, consumed in Task 7.
- `LOINC_WASO = "103215-0"` defined Task 7 Step 1, used Step 4; test asserts the literal `"103215-0"`.
- `wake_after_sleep_onset` / `minutes` names match across Tasks 6–7.
- Lexical system ordering (`http://` < `https://`) handled consistently in Task 1 and Task 4 assertions.
