-- source_credentials: OAuth tokens and refresh tokens per source.

CREATE TABLE source_credentials (
    source_name TEXT PRIMARY KEY,
    credentials_json TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);
