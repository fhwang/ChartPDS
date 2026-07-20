//! Analytical reads over the index.
//!
//! Each function is a generic primitive `(&SqlitePool, args) -> Result<T>`
//! — free functions, no shared state, no query-builder structs. The MCP
//! tool surface lives in the binary crate (`chartpds-mcp`), which composes
//! these primitives into named tools; nothing here knows about MCP.
//!
//! The `pub use` re-exports below are the full catalog; each query file's
//! docs carry its semantics. To add a primitive: new file here, `mod` +
//! `pub use` in this file, then `just prepare-sql` to cache its SQL.

mod aligned_table;
mod counts_per_code;
mod current_medications;
mod current_problems;
mod day_confidence;
mod episodes;
mod get_narrative;
mod latest_by_coding;
mod list_notifications;
mod observation_history;
mod observation_stats;
mod search_narratives;
mod signal_relationship;
#[cfg(test)]
mod test_support;

pub use aligned_table::{
    aligned_table, AlignedTable, AlignedTableError, AlignedTableParams, ColumnAggregate,
    ColumnSpec, EpisodeSpec, TableBucket, TableRow,
};
pub use counts_per_code::{counts_per_code, MetricSummary};
pub use current_medications::{current_medications, CurrentMedication, CurrentMedications};
pub use current_problems::{current_problems, CurrentProblem, CurrentProblems};
pub use day_confidence::{
    annotate_observations, resolve_source_day_confidence, roll_up_bucket_confidence,
    ObservationWithConfidence,
};
pub use get_narrative::{get_narrative, NarrativeCoding, NarrativeDetail};
pub use latest_by_coding::latest_by_coding;
pub use list_notifications::list_recent_notifications;
pub use observation_history::{observation_history, CodingKey};
pub use observation_stats::{
    observation_stats, BucketStats, ObservationStats, ObservationStatsError,
    ObservationStatsParams, StatsBucket, StatsField, StatsSummary, ThresholdCount,
};
pub use search_narratives::{search_narratives, NarrativeSearchHit};
pub use signal_relationship::{
    signal_relationship, GroupSummary, RelationshipBucket, RelationshipGroups, SignalRelationship,
    SignalRelationshipParams,
};
