CREATE TABLE notification_log (
    id INTEGER PRIMARY KEY,
    condition_id TEXT NOT NULL,
    fired_at TEXT NOT NULL,
    severity TEXT NOT NULL,
    title TEXT NOT NULL,
    message TEXT NOT NULL
);
CREATE INDEX idx_notification_log_fired_at ON notification_log(fired_at);
