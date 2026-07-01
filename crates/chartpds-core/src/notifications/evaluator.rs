//! Pure condition evaluator for notification triggers.
//!
//! All types and functions in this module are synchronous and have no database
//! or I/O dependencies, making them straightforward to test.

use time::OffsetDateTime;

/// Per-adapter state used as input for condition evaluation.
#[derive(Debug, Clone)]
pub struct AdapterConditionState {
    /// Machine-readable adapter name (e.g. `"fitbit"`).
    pub adapter_name: String,
    /// Human-readable display name (e.g. `"Fitbit"`).
    pub display_name: String,
    /// Whether the adapter's most recent error was an auth failure.
    pub auth_failed: bool,
    /// Number of consecutive sync failures for this adapter.
    pub consecutive_failures: i64,
    /// Wall-clock time the freshness frontier last advanced, if it ever has.
    ///
    /// `None` when the source has never produced data or the stored timestamp
    /// could not be parsed; either way the `frontier_stuck` condition will not
    /// fire.
    pub frontier_last_advanced_at: Option<OffsetDateTime>,
}

/// Aggregated input for the evaluator: one entry per configured adapter.
#[derive(Debug, Clone)]
pub struct ConditionsInput {
    /// Per-adapter state snapshots.
    pub adapters: Vec<AdapterConditionState>,
}

/// A notification to surface to the user.
#[derive(Debug, Clone)]
pub struct Notification {
    /// Condition that triggered the notification.
    pub condition_id: String,
    /// Severity level (`"critical"`, `"warning"`).
    pub severity: String,
    /// Short human-readable title.
    pub title: String,
    /// Longer descriptive message.
    pub message: String,
}

/// Result of evaluating a single condition.
#[derive(Debug, Clone)]
pub struct ConditionEvaluation {
    /// Unique condition identifier (e.g. `"auth_expired:fitbit"`).
    pub condition_id: String,
    /// Whether the condition is currently firing.
    pub is_firing: bool,
    /// The notification to dispatch, present only when `is_firing` is true.
    pub notification: Option<Notification>,
}

/// Number of consecutive sync failures before the `sync_failures` condition fires.
const SYNC_FAILURES_THRESHOLD: i64 = 3;

/// Hours the freshness frontier may stay unadvanced before `frontier_stuck` fires.
const FRONTIER_STUCK_THRESHOLD_HOURS: i64 = 48;

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

/// Evaluate the auth-expired condition for a single adapter.
fn eval_auth_expired(adapter: &AdapterConditionState) -> ConditionEvaluation {
    let condition_id = format!("auth_expired:{}", adapter.adapter_name);
    if adapter.auth_failed {
        ConditionEvaluation {
            condition_id: condition_id.clone(),
            is_firing: true,
            notification: Some(Notification {
                condition_id,
                severity: "critical".to_owned(),
                title: format!(
                    "ChartPDS: {} re-authorization required",
                    adapter.display_name
                ),
                message: format!(
                    "The {} adapter needs re-authorization. \
                     Use the get_google_health_auth_url and complete_google_health_auth \
                     tools to re-authorize.",
                    adapter.display_name
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

/// Evaluate the sync-failures condition for a single adapter.
fn eval_sync_failures(adapter: &AdapterConditionState) -> ConditionEvaluation {
    let condition_id = format!("sync_failures:{}", adapter.adapter_name);
    if adapter.consecutive_failures >= SYNC_FAILURES_THRESHOLD {
        ConditionEvaluation {
            condition_id: condition_id.clone(),
            is_firing: true,
            notification: Some(Notification {
                condition_id,
                severity: "warning".to_owned(),
                title: format!("ChartPDS: {} sync failing", adapter.display_name),
                message: format!(
                    "The {} adapter has failed {} consecutive sync attempts.",
                    adapter.display_name, adapter.consecutive_failures
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
    let stale = adapter
        .frontier_last_advanced_at
        .is_some_and(|advanced| (now - advanced).whole_hours() >= FRONTIER_STUCK_THRESHOLD_HOURS);

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

#[cfg(test)]
mod tests {
    use super::*;

    use time::format_description::well_known::Rfc3339;

    fn ts(s: &str) -> OffsetDateTime {
        OffsetDateTime::parse(s, &Rfc3339).expect("valid timestamp")
    }

    fn make_adapter(auth_failed: bool, consecutive_failures: i64) -> AdapterConditionState {
        AdapterConditionState {
            adapter_name: "fitbit".to_owned(),
            display_name: "Fitbit".to_owned(),
            auth_failed,
            consecutive_failures,
            frontier_last_advanced_at: None,
        }
    }

    #[test]
    fn auth_not_failed_does_not_fire() {
        let adapter = make_adapter(false, 0);
        let eval = eval_auth_expired(&adapter);
        assert!(!eval.is_firing);
        assert!(eval.notification.is_none());
        assert_eq!(eval.condition_id, "auth_expired:fitbit");
    }

    #[test]
    fn auth_failed_fires_critical() {
        let adapter = make_adapter(true, 0);
        let eval = eval_auth_expired(&adapter);
        assert!(eval.is_firing);
        let notif = eval.notification.as_ref().expect("notification present");
        assert_eq!(notif.severity, "critical");
        assert!(notif.title.contains("re-authorization required"));
        assert!(notif.title.contains("Fitbit"));
    }

    #[test]
    fn below_threshold_does_not_fire() {
        let adapter = make_adapter(false, 2);
        let eval = eval_sync_failures(&adapter);
        assert!(!eval.is_firing);
        assert!(eval.notification.is_none());
    }

    #[test]
    fn at_threshold_fires_warning() {
        let adapter = make_adapter(false, 3);
        let eval = eval_sync_failures(&adapter);
        assert!(eval.is_firing);
        let notif = eval.notification.as_ref().expect("notification present");
        assert_eq!(notif.severity, "warning");
        assert!(notif.title.contains("sync failing"));
        assert!(notif.title.contains("Fitbit"));
    }

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
        assert!(notif.message.contains("no new"));
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
}
