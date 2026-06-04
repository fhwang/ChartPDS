-- source_state: sync cursors and status per source.

CREATE TABLE source_state (
    source_name TEXT PRIMARY KEY,
    last_sync_at TEXT,
    last_sync_status TEXT,
    last_error_message TEXT,
    last_error_reason TEXT,
    last_synced_window_end TEXT,
    freshness_frontier_at TEXT,
    successful_ticks_since_frontier_advance INTEGER NOT NULL DEFAULT 0,
    consecutive_sync_failures INTEGER NOT NULL DEFAULT 0
);
