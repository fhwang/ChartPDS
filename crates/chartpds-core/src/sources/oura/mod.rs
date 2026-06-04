//! Oura ring adapter.
//!
//! Fetches sleep data from the Oura v2 API using a personal access token
//! (PAT), parses the per-epoch sleep-stage string into AASM-coded
//! observations, and archives the raw JSON response.

pub mod api;
pub mod confidence;
pub mod parser;
pub mod sleep_stage;
pub(crate) mod storage;
pub mod sync;

pub use sync::OuraSource;
