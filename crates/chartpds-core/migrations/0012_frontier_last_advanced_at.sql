-- Add a wall-clock "frontier last advanced" timestamp to source_state.
--
-- Replaces the dead `successful_ticks_since_frontier_advance` counter, which
-- was hardcoded to 0 on every write and never incremented. A wall-clock
-- timestamp is robust to the daemon's tick cadence and is what a future
-- "data not advancing" notification needs.
--
-- The old counter column is left in place (ignored) per the forward-only
-- migration policy; a follow-up forward migration may drop it.

ALTER TABLE source_state ADD COLUMN frontier_last_advanced_at TEXT;
