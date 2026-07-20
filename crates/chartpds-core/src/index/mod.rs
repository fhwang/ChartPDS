//! `SQLite` projection of the archive.
//!
//! Owns the schema (in `migrations/*.sql`), a `sqlx` connection pool, and
//! low-level CRUD per table. Consumers (`ingestion`, `queries`, `sync`) hold
//! a `sqlx::SqlitePool` produced by this module and call typed query
//! functions from here.
//!
//! `sqlx` compile-time SQL verification runs in **offline mode**: builds
//! read the committed `.sqlx/query-*.json` cache at the workspace root and
//! never connect to a live database. After changing `migrations/*.sql` or
//! any `sqlx::query!`/`sqlx::query_as!` invocation, run `just prepare-sql`
//! and commit the cache update alongside the change.
//!
//! Two column-type override patterns recur, because `SQLite` metadata is
//! sparse on nullability:
//!
//! - `INTEGER PRIMARY KEY` and `REFERENCES` columns are inferred as
//!   `Option<i64>`; force the non-null type with
//!   `RETURNING id AS "id!: i64"` (or `SELECT id AS "id!: i64"`).
//! - Nullable timestamp columns mapped to `OffsetDateTime` need the
//!   nullable suffix: `effective_end AS "effective_end?: OffsetDateTime"`.
//!
//! Migrations are forward-only: there are no down migrations. A wrong
//! migration is corrected by a follow-up forward migration — the
//! archive-as-truth model makes rebuilds cheap, so schema mistakes are
//! recoverable by re-ingest.

mod clear;
mod medications;
mod migrations;
mod narrative_texts;
mod notification_log;
mod notification_state;
mod observations;
mod pool;
mod problems;
mod source_credentials;
mod source_day_state;
mod source_documents;
mod source_state;

pub use clear::clear_ingested_data;
pub use medications::{
    insert as insert_medication, list_by_source_document as list_medications_by_source_document,
    InsertParams as InsertMedicationParams, Medication,
};
pub use migrations::run_migrations;
pub use narrative_texts::{
    get_by_source_document as get_narrative_text, set_title as set_narrative_title,
    upsert as upsert_narrative_text, NarrativeText, UpsertParams as UpsertNarrativeTextParams,
};
pub use notification_log::{
    append as append_notification_log, list_recent as list_recent_notification_log,
    NotificationLogEntry,
};
pub use notification_state::{
    get as get_notification_state, upsert as upsert_notification_state, NotificationStateRow,
};
pub use observations::{
    insert as insert_observation, list_by_source_document as list_observations_by_source_document,
    InsertParams as InsertObservationParams, Observation,
};
pub use pool::{open_pool, OpenError};
pub use problems::{
    insert as insert_problem, list_by_source_document as list_problems_by_source_document,
    InsertParams as InsertProblemParams, Problem,
};
pub use source_credentials::{
    get as get_source_credentials, upsert as upsert_source_credentials, SourceCredentials,
    UpsertParams as UpsertSourceCredentialsParams,
};
pub use source_day_state::{
    get as get_source_day_state, list_by_source as list_source_day_states_by_source,
    upsert as upsert_source_day_state, SourceDayState, UpsertParams as UpsertSourceDayStateParams,
};
pub use source_documents::{
    delete_by_id as delete_source_document,
    fetch_by_archive_key as fetch_source_document_by_archive_key,
    get_by_id as get_source_document_by_id, insert as insert_source_document,
    insert_superseding as insert_source_document_superseding,
    set_document_date as set_source_document_date, InsertParams as InsertSourceDocumentParams,
    SourceDocument, SupersedeOutcome,
};
pub use source_state::{
    get as get_source_state, upsert as upsert_source_state,
    upsert_sync_status as upsert_source_sync_status, SourceState,
    UpsertParams as UpsertSourceStateParams,
    UpsertSyncStatusParams as UpsertSourceSyncStatusParams,
};
