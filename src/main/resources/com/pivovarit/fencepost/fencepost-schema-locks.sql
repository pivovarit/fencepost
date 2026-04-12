CREATE TABLE IF NOT EXISTS fencepost_locks (
    lock_name   TEXT PRIMARY KEY,
    token       BIGINT NOT NULL DEFAULT 0,
    locked_by   TEXT,
    locked_at   TIMESTAMP WITH TIME ZONE,
    expires_at  TIMESTAMP WITH TIME ZONE
);
