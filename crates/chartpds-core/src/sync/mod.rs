//! Background sync daemon.
//!
//! When any source adapter is configured (Google Health OAuth credentials
//! for Fitbit, an Oura PAT), the MCP server spawns this daemon as a
//! `tokio::spawn`ed task that calls each adapter's `sync()` on a
//! configurable interval (default 5 minutes); it dies with the process.
//! `CHARTPDS_SYNC_INTERVAL_SECS` overrides the interval; `0` disables the
//! daemon entirely (manual `source_sync` tool calls only).
//!
//! After each tick the daemon records the result in `source_state`
//! (success/failure, consecutive-failure count, timestamps), then evaluates
//! notification conditions and dispatches any that fire (see
//! [`crate::notifications`]). The tick iterates over concrete source types
//! via `sync_one::<S: Source>()` — no `dyn Source`.

mod daemon;
mod tick;

pub use daemon::run_daemon;
pub use tick::TickDeps;
