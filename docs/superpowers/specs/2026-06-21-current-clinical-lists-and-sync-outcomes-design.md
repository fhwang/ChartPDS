# P1: Deduped clinical lists with provenance + structured sync outcomes

*Date: 2026-06-21. Status: approved design, pre-implementation.*

## Motivation

An external MCP client (a weekly health-summary routine) reports two
correctness gaps in the current tool surface. Neither blocks a tracked
metric, but both force the client to guess where the base layer should give
it facts:

1. **`list_problems` / `list_medications` return one row per ingested source
   document.** With ~12 clinical documents in the archive, every diagnosis
   and drug repeats ~12×, so the client must dedupe itself. Worse, the raw
   `status` field is unreliable in practice — every medication came back
   `suspended` and every problem `completed` — so "is the patient currently
   on a statin?" is unanswerable from the data.
2. **`sync_source` reports failure only as a generic MCP error.** The client
   cannot distinguish `reauth_required` from `no_credentials` from a
   transient network blip, so it cannot emit a precise action banner or
   decide whether to render against stale data.

## Guiding principle: faithful facts, not clinical judgment

ChartPDS is a **facts base layer** for an agentic layer above it. The base
layer's job is faithful aggregation over the archive, never clinical
interpretation. This is consistent with current industry guidance for
agent-native medical data substrates (immutable facts + provenance at the
base, inference at higher layers) and with FHIR US Core, which treats
"active" as a *derived/queried* notion and pushes deduplication and the
active/resolved judgment to the client.

Concretely:

- **Dedup is mechanical aggregation** (collapsing identical codes) — it
  stays in the base layer.
- **"Active vs resolved" is judgment under uncertainty** — it moves out to
  the agent.

So we do **not** add an `is_current` boolean or any interpreted status. We
expose the raw source-asserted `status` (clearly labeled untrustworthy) plus
*provenance facts* that let the agent make the call itself.

---

## Part 1 — Deduped problems/medications with provenance (request #4)

### 1.1 Prerequisite: a trustworthy per-document date

For provenance to mean anything, the agent needs to know *when* each
document pertains to. Today `source_documents` carries only `archived_at`
(when the blob entered our archive). A bulk import stamps every blob's
`archived_at` within seconds of each other, so it cannot order documents by
real-world recency.

The trustworthy date already exists per source:

- **CCDA** — `ClinicalDocument/effectiveTime` (the document's authored time;
  parseable via the existing `ccda/time.rs`).
- **Fitbit / Oura** — the archive manifest's `subject` attribute, which
  CLAUDE.md already documents as "the per-document replay date — Fitbit day
  / Oura sleep day."

So we add a single new column and populate it for **all** source types.

**Migration** (forward-only, per repo policy):

- Add `document_date TEXT` (nullable) to `source_documents`. Stores an
  ISO-8601 date (or RFC 3339 timestamp where available). Nullable because a
  CCDA may omit `effectiveTime`; in practice it is populated for every
  source.

**Semantics:** `document_date` = "the date this document pertains to." For
CCDA that is the authored snapshot date; for periodic sources it is the data
day. Coherent across all three under a single name.

### 1.2 Populate `document_date` on ingest

- **CCDA ingest** — extract `ClinicalDocument/effectiveTime`, store as
  `document_date` on the `source_documents` row. If absent, leave null.
- **Fitbit / Oura ingest + rebuild-replay** — stamp `document_date` from the
  per-document day already known to the adapter (live sync) or carried in
  the manifest `subject` (rebuild). Thread that already-known date into the
  `source_documents` insert.
- **Rebuild** re-derives `document_date` from the blobs/manifests, exactly
  like every other re-ingested field; `archived_at` remains preserved from
  the manifest and is never re-stamped.

### 1.3 New query primitives

Two new pure async functions under `crates/chartpds-core/src/queries/`,
following the existing one-function-per-file pattern with `mod` + `pub use`
re-exports in `queries/mod.rs`:

- `current_problems(&SqlitePool) -> Result<CurrentProblems, sqlx::Error>`
- `current_medications(&SqlitePool) -> Result<CurrentMedications, sqlx::Error>`

Each:

1. Groups `problems` / `medications` rows by `(coding_system, coding_code)`.
   Problems and medications only ever originate from CCDA, so every joined
   `source_documents` row has a CCDA-origin `document_date`; null
   Fitbit/Oura rows are never read by these queries.
2. Selects the **winning row** — the one from the most-recent document
   (max `document_date`) for that code, breaking ties by max
   `source_documents.id` for determinism. A null `document_date` sorts as
   oldest (SQLite orders NULL below any value), so a dated document always
   wins over an undated one. The winning row carries its source-asserted
   fields:
   - problems: `coding_display`, `status`, `onset_date`
   - medications: `coding_display`, `status`, `dose`, `route`, `start_date`,
     `end_date`
3. Aggregates provenance for the code across all documents:
   - `document_count` — number of documents mentioning it
   - `first_seen` — min `document_date` (SQL `MIN` ignores nulls)
   - `last_seen` — max `document_date` (SQL `MAX` ignores nulls)

The return type carries a top-level reference fact plus the items:

```rust
pub struct CurrentProblems {
    /// Newest document_date across all source documents — the agent's
    /// reference point for judging recency.
    pub latest_document_date: Option<String>,
    pub items: Vec<CurrentProblem>,
}

pub struct CurrentProblem {
    pub coding_system: String,
    pub coding_code: String,
    pub coding_display: Option<String>,
    /// Source-asserted CCDA statusCode. UNRELIABLE — do not treat as
    /// active/resolved truth; use provenance fields to judge currency.
    pub status: String,
    pub onset_date: Option<String>,
    pub document_count: i64,
    pub first_seen: Option<String>,
    pub last_seen: Option<String>,
}
```

`CurrentMedications` / `CurrentMedication` mirror this, adding `dose`,
`route`, `start_date`, `end_date` to each item.

### 1.4 Tool surface

`list_problems` / `list_medications` are **replaced in place** (single
consumer; the raw per-document dump is retired). Each returns the new
envelope:

```jsonc
{
  "latest_document_date": "2026-06-10",
  "items": [
    { "coding_system": "http://snomed.info/sct", "coding_code": "44054006",
      "coding_display": "Type 2 diabetes mellitus",
      "status": "completed",            // source-asserted, unreliable
      "onset_date": "2020-03-15",
      "document_count": 7,
      "first_seen": "2021-03-01", "last_seen": "2024-01-15" }
    // medications additionally: dose, route, start_date, end_date
  ]
}
```

The tool **description** states explicitly that `status` is source-asserted
and unreliable, and instructs the agent to derive currency by comparing
`last_seen` against `latest_document_date` (and, for medications, an
`end_date` in the past as a discontinuation signal).

### 1.5 sqlx + verification

Run `just prepare-sql` after the migration and the new `sqlx::query!`
invocations so the offline cache stays hermetic; commit `.sqlx/` changes
with the code. Run `just check`.

---

## Part 2 — Structured sync outcomes (request #5)

Most machinery already exists: `error_reason_code()` in `sync/tick.rs`
already maps the `sources::Error` enum to reason codes
(`reauth_required`, `transient`, `parse_error`, `archive_error`,
`database_error`) and stores them in `source_state.last_error_reason`. Two
gaps remain: the MCP tool collapses everything into a generic `McpError`,
and missing credentials masquerade as `reauth_required`.

### 2.1 New error variant: `NoCredentials`

- Add `sources::Error::NoCredentials { reason }` to
  `crates/chartpds-core/src/sources/error.rs` (the enum is already
  `#[non_exhaustive]`).
- Add a `"no_credentials"` arm to `error_reason_code()` in `sync/tick.rs`.
- Switch the missing-credentials paths from `ReauthRequired` to
  `NoCredentials`:
  - Fitbit `sources/fitbit/sync.rs` (the no-credentials-found branch).
  - Oura `sources/oura/sync.rs` (add an explicit no-token check).
- **Notifications unchanged:** the evaluator's `auth_failed` still keys on
  `last_error_reason == "reauth_required"`, so `auth_expired` fires only for
  a genuine expiry — a never-connected source correctly does *not* demand
  reauth. `no_credentials` does not trigger an auth notification.

### 2.2 `sync_source` returns structured results

A sync *failure* becomes a **successful tool call** whose content reports the
failure, so the agent always receives parseable JSON and can decide whether
to render against stale data. `McpError` is reserved for truly internal
faults (e.g. JSON serialization of the result).

The tool always returns a uniform `results` array — single-source syncs
return a one-element array, so the agent never branches on shape:

```jsonc
{
  "results": [
    { "source": "fitbit", "ok": true,  "days_synced": 8, "total_samples": 12000 },
    { "source": "oura",   "ok": false, "reason": "reauth_required",
      "message": "re-authorization required: token endpoint returned 401: ..." }
  ]
}
```

- On success: `{ source, ok: true, days_synced, total_samples }`.
- On failure: `{ source, ok: false, reason, message }`, where
  `reason ∈ { reauth_required, no_credentials, transient, parse_error,
  archive_error, database_error }` and `message` is the human-readable
  `Error::to_string()`.

This replaces the current behavior where `sync_fitbit_inner` /
`sync_oura_inner` raise `McpError::internal_error("sync failed: …")` and the
multi-source path accumulates error strings.

### 2.3 Verification

No schema change in Part 2. Run `just check` (the new error variant requires
exhaustive-match updates wherever `sources::Error` is matched).

---

## Out of scope

P0 items (already shipped) and all P2 items: richer metric discovery
(`list_metrics`), multi-coding observation history, document-level archive
listing (`list_documents`), and WASO indexing. The `document_date` column
added here pre-positions a future `list_documents` freshness check but we do
not build that tool now.

## Testing

- **`document_date` extraction** — unit tests that CCDA ingest captures
  `effectiveTime`, and that Fitbit/Oura ingest + rebuild-replay stamp the
  per-document day. A null-`effectiveTime` CCDA leaves the column null
  without failing ingest.
- **Dedup + provenance** — tests with the same code across multiple
  documents of differing `document_date`: assert one item per code, winning
  fields taken from the newest document, and correct `document_count` /
  `first_seen` / `last_seen` / `latest_document_date`.
- **Structured sync** — construct the server directly in `#[tokio::test]`
  (per CLAUDE.md, no stdio transport needed) and assert the `results`-array
  shape for success and for each failure reason, including the new
  `no_credentials` path.
- **`error_reason_code`** — unit test the new `NoCredentials` →
  `"no_credentials"` mapping.
- `just check` is the completion gate.
