-- source_day_state: per-source per-day ingestion bookkeeping.

CREATE TABLE source_day_state (
    source_name TEXT NOT NULL,
    date TEXT NOT NULL,
    samples_count INTEGER NOT NULL,
    samples_count_prev INTEGER,
    last_pulled_at TEXT NOT NULL,
    PRIMARY KEY (source_name, date)
);
