//! Oura ring adapter.
//!
//! Fetches sleep data from the Oura v2 API using a personal access token
//! (PAT), parses the per-epoch sleep-stage string into AASM-coded
//! observations, and archives the raw JSON response.
//!
//! Setup: generate a PAT at
//! <https://cloud.ouraring.com/personal-access-tokens>, then either set
//! `OURA_PERSONAL_ACCESS_TOKEN` in the server's environment or call the
//! `source_connect` MCP tool with `source="oura"` and the token;
//! `source_sync` pulls recent sleep sessions.
//!
//! Each 5-minute epoch becomes one `aasm-sleep-stage` observation (see
//! [`sleep_stage`] for the character mapping) with `value_string` the stage
//! name (e.g. `"n3"`) and `value_quantity` the stage discriminant. For each
//! `long_sleep` night the adapter also derives two nightly summary
//! observations from the epoch stream: total sleep time (LOINC `93832-4`,
//! minutes) and wake-after-sleep-onset (LOINC `103215-0`, minutes awake
//! after sleep onset).

pub mod api;
pub mod confidence;
pub mod parser;
pub mod sleep_stage;
pub(crate) mod storage;
pub mod sync;

pub use sync::OuraSource;
