//! Analytical reads over the index.
//!
//! Each function is a generic primitive that future MCP tools and CLI
//! subcommands will compose into named queries. No tool surface lives
//! here; this module is read-shaped (free functions, no state) and
//! deliberately small.

mod counts_per_code;
mod current_medications;
mod current_problems;
mod day_confidence;
mod duration_in_value_range;
mod get_narrative;
mod latest_by_code;
mod list_notifications;
mod longest_continuous_in_value_range;
mod observation_history;
mod search_narratives;
#[cfg(test)]
mod test_support;

pub use counts_per_code::{counts_per_code, MetricSummary};
pub use current_medications::{current_medications, CurrentMedication, CurrentMedications};
pub use current_problems::{current_problems, CurrentProblem, CurrentProblems};
pub use day_confidence::{
    annotate_observations, resolve_source_day_confidence, roll_up_bucket_confidence,
    ObservationWithConfidence,
};
pub use duration_in_value_range::{
    duration_in_value_range, Bucket, BucketMinutes, DurationInRange, DurationInRangeError,
    DurationInValueRangeParams,
};
pub use get_narrative::{get_narrative, NarrativeCoding, NarrativeDetail};
pub use latest_by_code::latest_by_code;
pub use list_notifications::list_recent_notifications;
pub use longest_continuous_in_value_range::{
    longest_continuous_in_value_range, BucketLongest, LongestContinuousInRange,
    LongestContinuousParams,
};
pub use observation_history::{observation_history, CodingKey};
pub use search_narratives::{search_narratives, NarrativeSearchHit};
