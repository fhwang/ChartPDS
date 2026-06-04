//! External data sources (adapters).
//!
//! Each source pulls data from an external API, archives the raw response,
//! and inserts structured rows into the index. Shared infrastructure lives
//! here (`oauth`, `error`, `source_trait`); per-source code lives in
//! submodules (`fitbit/`, `oura/`).

pub mod confidence;
mod error;
pub mod fitbit;
pub mod oauth;
pub mod oura;
mod source_trait;

pub use confidence::{ConfidenceByDate, DayConfidence};
pub use error::{Error, Result};
pub use source_trait::Source;

/// Summary of a sync run.
#[derive(Debug)]
pub struct SyncResult {
    /// Number of days (or sessions) successfully synced (new data ingested).
    pub days_synced: i64,
    /// Total number of samples (observations) ingested across all days.
    pub total_samples: i64,
}
