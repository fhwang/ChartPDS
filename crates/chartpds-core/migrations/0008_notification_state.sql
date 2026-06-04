CREATE TABLE notification_state (
    condition_id TEXT PRIMARY KEY,
    last_fired_at TEXT,
    last_state TEXT NOT NULL
);
