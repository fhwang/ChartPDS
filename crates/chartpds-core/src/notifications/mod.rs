//! Notification system: condition evaluation, dispatch, and logging.
//!
//! The evaluator ([`evaluator`]) is pure (no I/O) and produces
//! [`ConditionEvaluation`]s from adapter state snapshots. The dispatcher
//! ([`dispatch`]) writes fired notifications to the database and enforces
//! a 24-hour re-fire cadence so the same condition does not spam the log
//! (unless the condition resolves and fires again). Suppression state lives
//! in the `notification_state` table; fired notifications are appended to
//! `notification_log`, surfaced by the `notification_list` MCP tool.
//!
//! Two conditions are evaluated after every sync tick:
//!
//! - `auth_expired:{adapter}` — severity `"critical"`, fires when the
//!   adapter's most recent error reason is `reauth_required`.
//! - `sync_failures:{adapter}` — severity `"warning"`, fires when the
//!   adapter has >= 3 consecutive sync failures.

mod dispatch;
mod evaluator;

pub use dispatch::evaluate_and_dispatch;
pub use evaluator::{
    evaluate_all, AdapterConditionState, ConditionEvaluation, ConditionsInput, Notification,
};
