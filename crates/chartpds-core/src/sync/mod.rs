//! Background sync daemon.
//!
//! Runs adapter sync ticks on a configurable interval alongside the MCP
//! server. Today only the Fitbit adapter is registered; future adapters
//! get added to the tick function.

mod daemon;
mod tick;

pub use daemon::run_daemon;
pub use tick::TickDeps;
