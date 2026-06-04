//! Pure condition evaluator for notification triggers.
//!
//! All types and functions in this module are synchronous and have no database
//! or I/O dependencies, making them straightforward to test.

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

/// Evaluate all conditions for the given input.
///
/// Returns one [`ConditionEvaluation`] per condition per adapter.
#[must_use]
pub fn evaluate_all(input: &ConditionsInput) -> Vec<ConditionEvaluation> {
    let mut out = Vec::new();
    for adapter in &input.adapters {
        out.push(eval_auth_expired(adapter));
        out.push(eval_sync_failures(adapter));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_adapter(auth_failed: bool, consecutive_failures: i64) -> AdapterConditionState {
        AdapterConditionState {
            adapter_name: "fitbit".to_owned(),
            display_name: "Fitbit".to_owned(),
            auth_failed,
            consecutive_failures,
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
    fn evaluate_all_returns_two_conditions_per_adapter() {
        let input = ConditionsInput {
            adapters: vec![
                AdapterConditionState {
                    adapter_name: "fitbit".to_owned(),
                    display_name: "Fitbit".to_owned(),
                    auth_failed: true,
                    consecutive_failures: 5,
                },
                AdapterConditionState {
                    adapter_name: "oura".to_owned(),
                    display_name: "Oura".to_owned(),
                    auth_failed: false,
                    consecutive_failures: 0,
                },
            ],
        };
        let evals = evaluate_all(&input);
        assert_eq!(evals.len(), 4);

        // Fitbit: both conditions fire.
        assert!(evals[0].is_firing); // auth_expired:fitbit
        assert!(evals[1].is_firing); // sync_failures:fitbit

        // Oura: neither fires.
        assert!(!evals[2].is_firing); // auth_expired:oura
        assert!(!evals[3].is_firing); // sync_failures:oura
    }
}
