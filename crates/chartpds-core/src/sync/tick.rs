//! Single sync tick: run all adapters and record results.

use sqlx::SqlitePool;

use crate::archive::Archive;
use crate::index;
use crate::notifications::{evaluate_and_dispatch, AdapterConditionState, ConditionsInput};
use crate::sources;
use crate::sources::fitbit::FitbitSource;
use crate::sources::oura::OuraSource;
use crate::sources::Source;

/// Dependencies required by the sync tick.
///
/// All fields are cheaply cloneable (`Arc`-backed or handle types).
/// Optional source fields allow partial configuration — the daemon
/// only syncs sources that are present.
pub struct TickDeps {
    /// Blob archive for storing raw API responses.
    pub archive: Archive,
    /// `SQLite` connection pool for the index.
    pub pool: SqlitePool,
    /// Fitbit/Google Health source (requires OAuth credentials).
    pub fitbit: Option<FitbitSource>,
    /// Oura ring source (requires personal access token).
    pub oura: Option<OuraSource>,
}

/// Sync a single source, recording success/failure in `source_state`.
async fn sync_one<S: Source>(source: &S, archive: &Archive, pool: &SqlitePool) {
    match source.sync(archive, pool, 8).await {
        Ok(result) => {
            tracing::info!(
                source = source.name(),
                days = result.days_synced,
                samples = result.total_samples,
                "sync tick completed"
            );
            record_sync_success(pool, source.name()).await;
        }
        Err(err) => {
            tracing::warn!(source = source.name(), %err, "sync tick failed");
            record_sync_error(pool, source.name(), &err).await;
        }
    }
}

/// Build an [`AdapterConditionState`] snapshot for one source.
async fn adapter_condition(pool: &SqlitePool, source: &dyn SourceMeta) -> AdapterConditionState {
    let state = index::get_source_state(pool, source.name())
        .await
        .ok()
        .flatten();
    AdapterConditionState {
        adapter_name: source.name().to_owned(),
        display_name: source.display_name().to_owned(),
        auth_failed: state
            .as_ref()
            .is_some_and(|s| s.last_error_reason.as_deref() == Some("reauth_required")),
        consecutive_failures: state.as_ref().map_or(0, |s| s.consecutive_sync_failures),
    }
}

/// Minimal trait object for reading source metadata without `async`.
///
/// The full [`Source`] trait is not object-safe (it returns `impl Future`),
/// so we use this subset for building notification condition snapshots.
trait SourceMeta: Send + Sync {
    /// Short identifier.
    fn name(&self) -> &'static str;
    /// Human-readable name.
    fn display_name(&self) -> &'static str;
}

impl<T: Source> SourceMeta for T {
    fn name(&self) -> &'static str {
        Source::name(self)
    }
    fn display_name(&self) -> &'static str {
        Source::display_name(self)
    }
}

/// Run one sync cycle: call each configured adapter and record the result.
///
/// After syncing, evaluates notification conditions across all adapters.
pub async fn run_tick(deps: &TickDeps) {
    if let Some(ref fitbit) = deps.fitbit {
        sync_one(fitbit, &deps.archive, &deps.pool).await;
    }
    if let Some(ref oura) = deps.oura {
        sync_one(oura, &deps.archive, &deps.pool).await;
    }

    // Evaluate notification conditions after all sync attempts.
    let mut adapters = Vec::new();
    if let Some(ref fitbit) = deps.fitbit {
        adapters.push(adapter_condition(&deps.pool, fitbit).await);
    }
    if let Some(ref oura) = deps.oura {
        adapters.push(adapter_condition(&deps.pool, oura).await);
    }
    let input = ConditionsInput { adapters };
    evaluate_and_dispatch(&deps.pool, &input).await;
}

/// Record a successful sync in `source_state`.
///
/// Sets `last_sync_status = "success"`, resets `consecutive_sync_failures`
/// to 0, and updates `last_sync_at`.
pub(crate) async fn record_sync_success(pool: &SqlitePool, source_name: &str) {
    let now = now_rfc3339();
    if let Err(err) = index::upsert_source_state(
        pool,
        index::UpsertSourceStateParams {
            source_name,
            last_sync_at: Some(&now),
            last_sync_status: Some("success"),
            last_error_message: None,
            last_error_reason: None,
            last_synced_window_end: None,
            freshness_frontier_at: None,
            successful_ticks_since_frontier_advance: 0,
            consecutive_sync_failures: 0,
        },
    )
    .await
    {
        tracing::error!(%err, source = source_name, "failed to record sync success in source_state");
    }
}

/// Record a failed sync in `source_state`.
///
/// Sets `last_sync_status = "error"`, stores the error message and a
/// machine-readable reason code, and increments `consecutive_sync_failures`.
pub(crate) async fn record_sync_error(pool: &SqlitePool, source_name: &str, err: &sources::Error) {
    let now = now_rfc3339();
    let error_message = err.to_string();
    let error_reason = error_reason_code(err);

    // Read current state to get the existing consecutive_sync_failures count.
    let current_failures = match index::get_source_state(pool, source_name).await {
        Ok(Some(state)) => state.consecutive_sync_failures,
        Ok(None) => 0,
        Err(db_err) => {
            tracing::error!(
                %db_err,
                source = source_name,
                "failed to read source_state for failure increment"
            );
            0
        }
    };

    if let Err(db_err) = index::upsert_source_state(
        pool,
        index::UpsertSourceStateParams {
            source_name,
            last_sync_at: Some(&now),
            last_sync_status: Some("error"),
            last_error_message: Some(&error_message),
            last_error_reason: Some(error_reason),
            last_synced_window_end: None,
            freshness_frontier_at: None,
            successful_ticks_since_frontier_advance: 0,
            consecutive_sync_failures: current_failures + 1,
        },
    )
    .await
    {
        tracing::error!(
            %db_err,
            source = source_name,
            "failed to record sync error in source_state"
        );
    }
}

/// Map an adapter error variant to a machine-readable reason code.
fn error_reason_code(err: &sources::Error) -> &'static str {
    err.reason_code()
}

/// Get the current UTC time as an RFC 3339 string.
fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::open_pool;

    async fn fresh_pool() -> SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        // Leak the temp dir so the file lives as long as the pool.
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    #[tokio::test]
    async fn record_sync_success_sets_status_and_resets_failures() {
        let pool = fresh_pool().await;

        // Seed with one prior failure so we can verify it resets.
        index::upsert_source_state(
            &pool,
            index::UpsertSourceStateParams {
                source_name: "fitbit",
                last_sync_at: Some("2026-01-01T00:00:00Z"),
                last_sync_status: Some("error"),
                last_error_message: Some("something broke"),
                last_error_reason: Some("transient"),
                last_synced_window_end: None,
                freshness_frontier_at: None,
                successful_ticks_since_frontier_advance: 0,
                consecutive_sync_failures: 3,
            },
        )
        .await
        .expect("seed upsert");

        record_sync_success(&pool, "fitbit").await;

        let state = index::get_source_state(&pool, "fitbit")
            .await
            .expect("get succeeds")
            .expect("row exists");

        assert_eq!(state.last_sync_status.as_deref(), Some("success"));
        assert_eq!(state.consecutive_sync_failures, 0);
        assert!(state.last_sync_at.is_some());
    }

    #[tokio::test]
    async fn record_sync_error_increments_consecutive_failures() {
        let pool = fresh_pool().await;

        let err = sources::Error::ReauthRequired {
            reason: "token expired".to_owned(),
        };

        // First error: should go from 0 to 1.
        record_sync_error(&pool, "fitbit", &err).await;

        let state = index::get_source_state(&pool, "fitbit")
            .await
            .expect("get succeeds")
            .expect("row exists");

        assert_eq!(state.last_sync_status.as_deref(), Some("error"));
        assert_eq!(state.consecutive_sync_failures, 1);
        assert_eq!(state.last_error_reason.as_deref(), Some("reauth_required"));
        assert!(state
            .last_error_message
            .as_ref()
            .is_some_and(|m| m.contains("token expired")));

        // Second error: should go from 1 to 2.
        let err2 = sources::Error::Transient {
            reason: "network timeout".to_owned(),
        };
        record_sync_error(&pool, "fitbit", &err2).await;

        let state2 = index::get_source_state(&pool, "fitbit")
            .await
            .expect("get succeeds")
            .expect("row exists");

        assert_eq!(state2.consecutive_sync_failures, 2);
        assert_eq!(state2.last_error_reason.as_deref(), Some("transient"));
    }
}
