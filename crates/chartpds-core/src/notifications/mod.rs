//! Notification system: condition evaluation, dispatch, and logging.
//!
//! The evaluator ([`evaluator`]) is pure (no I/O) and produces
//! [`ConditionEvaluation`]s from adapter state snapshots. The dispatcher
//! ([`dispatch`]) writes fired notifications to the database and enforces
//! a 24-hour re-fire cadence so the same condition does not spam the log.

mod dispatch;
mod evaluator;

pub use dispatch::evaluate_and_dispatch;
pub use evaluator::{
    evaluate_all, AdapterConditionState, ConditionEvaluation, ConditionsInput, Notification,
};
