//! The `Source` trait: shared interface for external data adapters.
//!
//! Each source (Fitbit, Oura, etc.) fetches data from an external API,
//! archives the raw response, and inserts structured observations into
//! the index. Auth mechanics differ between sources (OAuth for Fitbit,
//! PAT for Oura), so each source owns its credentials internally; the
//! sync method only needs the archive and database pool.

use crate::archive::Archive;
use crate::sources::{Result, SyncResult};
use sqlx::SqlitePool;

/// A data source that can sync recent data into the index.
///
/// Implemented by concrete adapter structs (`FitbitSource`, `OuraSource`).
/// The trait uses native `impl Future` return types (stable since Rust 1.75).
/// This means `dyn Source` is not object-safe, but we only have a small
/// fixed set of sources and iterate over them concretely in the daemon
/// tick — no runtime polymorphism needed.
pub trait Source: Send + Sync {
    /// Short identifier (e.g. `"fitbit"`, `"oura"`).
    fn name(&self) -> &'static str;

    /// Human-readable name for notifications and UI.
    fn display_name(&self) -> &'static str;

    /// Pull recent data and insert into the index.
    ///
    /// Each source handles its own auth refresh (or uses a long-lived
    /// token). The caller provides the archive for blob storage and
    /// the pool for index writes.
    fn sync(
        &self,
        archive: &Archive,
        pool: &SqlitePool,
        window_days: i64,
    ) -> impl std::future::Future<Output = Result<SyncResult>> + Send;
}
