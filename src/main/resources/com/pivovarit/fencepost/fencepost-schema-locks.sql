CREATE TABLE IF NOT EXISTS fencepost_locks (
    lock_name   TEXT PRIMARY KEY,
    lock_type   TEXT NOT NULL,
    token       BIGINT NOT NULL DEFAULT 0,
    locked_by   TEXT,
    locked_at   TIMESTAMP WITH TIME ZONE,
    expires_at  TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS fencepost_locks_tokens (
    lock_name       TEXT PRIMARY KEY,
    token           BIGINT NOT NULL DEFAULT 0,
    last_locked_by  TEXT,
    last_locked_at  TIMESTAMP WITH TIME ZONE
);

CREATE SEQUENCE IF NOT EXISTS fencepost_locks_token_seq;
