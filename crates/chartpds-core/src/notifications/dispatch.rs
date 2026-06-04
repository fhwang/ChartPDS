//! Dispatch logic: fire-or-skip with 24-hour re-fire cadence.
//!
//! After the evaluator produces [`ConditionEvaluation`]s, this module
//! decides which notifications actually get written to the log. It
//! enforces a 24-hour re-fire cadence: a condition that is continuously
//! firing will only appear in the notification log once per day.

use sqlx::SqlitePool;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::index;
use crate::index::NotificationStateRow;

use super::evaluate_all;
use super::evaluator::{ConditionEvaluation, ConditionsInput};

/// Re-fire cadence: suppress duplicate notifications within this duration.
const REFIRE_CADENCE_HOURS: i64 = 24;

/// Evaluate all conditions and dispatch notifications that should fire.
///
/// Swallows per-condition errors (logs a warning) so that one failed
/// condition does not block the others.
pub async fn evaluate_and_dispatch(pool: &SqlitePool, input: &ConditionsInput) {
    for eval in evaluate_all(input) {
        if let Err(err) = maybe_fire(pool, &eval).await {
            tracing::warn!(
                %err,
                condition = %eval.condition_id,
                "notification dispatch error"
            );
        }
    }
}

/// Decide whether to fire (or resolve) a single condition evaluation.
async fn maybe_fire(pool: &SqlitePool, eval: &ConditionEvaluation) -> Result<(), sqlx::Error> {
    let prior = index::get_notification_state(pool, &eval.condition_id).await?;

    if eval.is_firing {
        if should_refire(prior.as_ref()) {
            let now = now_rfc3339();
            let notif = eval
                .notification
                .as_ref()
                .expect("firing evaluation always has a notification");

            index::append_notification_log(
                pool,
                &notif.condition_id,
                &now,
                &notif.severity,
                &notif.title,
                &notif.message,
            )
            .await?;

            index::upsert_notification_state(pool, &eval.condition_id, Some(&now), "firing")
                .await?;

            tracing::warn!(
                condition = %eval.condition_id,
                severity = %notif.severity,
                title = %notif.title,
                "notification fired"
            );
        }
    } else if prior.as_ref().is_some_and(|p| p.last_state == "firing") {
        index::upsert_notification_state(pool, &eval.condition_id, None, "resolved").await?;
    }

    Ok(())
}

/// Determine whether a firing condition should (re-)fire given its prior state.
///
/// Returns `true` when:
/// - There is no prior state (first time this condition was seen).
/// - The prior state is `"resolved"` (condition re-entered firing).
/// - More than 24 hours have elapsed since the last fire.
fn should_refire(prior: Option<&NotificationStateRow>) -> bool {
    let Some(state) = prior else {
        return true;
    };

    if state.last_state != "firing" {
        return true;
    }

    let Some(fired_at_str) = &state.last_fired_at else {
        return true;
    };

    let Ok(last_fired) = OffsetDateTime::parse(fired_at_str, &Rfc3339) else {
        // If we can't parse the timestamp, re-fire to be safe.
        return true;
    };

    let now = OffsetDateTime::now_utc();
    let elapsed = now - last_fired;
    elapsed.whole_hours() >= REFIRE_CADENCE_HOURS
}

/// Get the current UTC time as an RFC 3339 string.
fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notifications::{AdapterConditionState, ConditionsInput};

    async fn fresh_pool() -> SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        index::open_pool(&url).await.expect("open pool")
    }

    fn firing_input() -> ConditionsInput {
        ConditionsInput {
            adapters: vec![AdapterConditionState {
                adapter_name: "fitbit".to_owned(),
                display_name: "Fitbit".to_owned(),
                auth_failed: true,
                consecutive_failures: 0,
            }],
        }
    }

    fn resolved_input() -> ConditionsInput {
        ConditionsInput {
            adapters: vec![AdapterConditionState {
                adapter_name: "fitbit".to_owned(),
                display_name: "Fitbit".to_owned(),
                auth_failed: false,
                consecutive_failures: 0,
            }],
        }
    }

    #[tokio::test]
    async fn fire_condition_creates_log_entry() {
        let pool = fresh_pool().await;
        evaluate_and_dispatch(&pool, &firing_input()).await;

        let entries = index::list_recent_notification_log(&pool, 10)
            .await
            .expect("list");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].condition_id, "auth_expired:fitbit");
        assert_eq!(entries[0].severity, "critical");
    }

    #[tokio::test]
    async fn refire_suppressed_within_cadence() {
        let pool = fresh_pool().await;
        evaluate_and_dispatch(&pool, &firing_input()).await;
        evaluate_and_dispatch(&pool, &firing_input()).await;

        let entries = index::list_recent_notification_log(&pool, 10)
            .await
            .expect("list");
        // Only 1 entry — the second fire was suppressed.
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn fire_after_resolve_creates_second_entry() {
        let pool = fresh_pool().await;

        // First fire.
        evaluate_and_dispatch(&pool, &firing_input()).await;

        // Resolve.
        evaluate_and_dispatch(&pool, &resolved_input()).await;

        // Fire again — should create a second entry because prior was resolved.
        evaluate_and_dispatch(&pool, &firing_input()).await;

        let entries = index::list_recent_notification_log(&pool, 10)
            .await
            .expect("list");
        assert_eq!(entries.len(), 2);
    }
}
