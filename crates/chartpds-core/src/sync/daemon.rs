//! Background sync daemon loop.

use std::time::Duration;

use super::tick::{run_tick, TickDeps};

/// Run the sync daemon loop on a configurable interval.
///
/// Skips the first immediate tick (lets the MCP server stabilize), then
/// calls [`run_tick`] on each subsequent interval. The loop runs
/// indefinitely; it is cancelled when the tokio runtime drops (i.e. when
/// the MCP server's `waiting()` returns and `main` exits).
pub async fn run_daemon(deps: TickDeps, interval_secs: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    // Skip the first immediate tick.
    interval.tick().await;

    loop {
        interval.tick().await;
        tracing::info!("sync daemon: starting tick");
        run_tick(&deps).await;
    }
}
