//! Analytical reads over the index.
//!
//! Each function is a generic primitive that future MCP tools and CLI
//! subcommands will compose into named queries. No tool surface lives
//! here; this module is read-shaped (free functions, no state) and
//! deliberately small.

mod counts_per_code;
mod in_range;
mod latest_by_code;
mod list_medications;
mod list_notifications;
mod list_problems;
#[cfg(test)]
mod test_support;

pub use counts_per_code::{counts_per_code, CodeCount};
pub use in_range::in_range;
pub use latest_by_code::latest_by_code;
pub use list_medications::list_medications;
pub use list_notifications::list_recent_notifications;
pub use list_problems::list_problems;
