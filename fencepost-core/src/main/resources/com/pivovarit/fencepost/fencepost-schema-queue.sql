CREATE TABLE IF NOT EXISTS fencepost_queue (
    id          BIGSERIAL PRIMARY KEY,
    queue_name  TEXT NOT NULL,
    payload     BYTEA NOT NULL,
    type        TEXT,
    headers     JSONB,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    visible_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    attempts    INT NOT NULL DEFAULT 0,
    picked_by   TEXT
);

CREATE INDEX IF NOT EXISTS idx_fencepost_queue_dequeue
    ON fencepost_queue (queue_name, visible_at);
