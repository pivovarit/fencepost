package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public final class TestSchema {

    private TestSchema() {
    }

    public static void resetLocks(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute(
              "DO $$ DECLARE r RECORD;"
                + " BEGIN FOR r IN SELECT sequencename FROM pg_sequences WHERE sequencename LIKE 'fencepost_st_%'"
                + " LOOP EXECUTE 'DROP SEQUENCE IF EXISTS ' || r.sequencename; END LOOP; END$$;"
                + " DROP TABLE IF EXISTS fencepost_locks_tokens;"
                + " DROP TABLE IF EXISTS fencepost_locks;"
                + " DROP SEQUENCE IF EXISTS fencepost_locks_token_seq;"
                + " CREATE TABLE fencepost_locks ("
                + "   lock_name TEXT PRIMARY KEY,"
                + "   lock_type TEXT NOT NULL,"
                + "   token BIGINT NOT NULL DEFAULT 0,"
                + "   locked_by TEXT,"
                + "   locked_at TIMESTAMPTZ,"
                + "   expires_at TIMESTAMPTZ"
                + " );"
                + " CREATE TABLE fencepost_locks_tokens ("
                + "   lock_name TEXT PRIMARY KEY,"
                + "   token BIGINT NOT NULL DEFAULT 0,"
                + "   last_locked_by TEXT,"
                + "   last_locked_at TIMESTAMPTZ"
                + " );"
                + " CREATE SEQUENCE fencepost_locks_token_seq"
            );
        }
    }

    public static void resetQueue(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute(
              "DROP TABLE IF EXISTS fencepost_queue;"
                + " CREATE TABLE fencepost_queue ("
                + "   id BIGSERIAL PRIMARY KEY,"
                + "   queue_name TEXT NOT NULL,"
                + "   payload BYTEA NOT NULL,"
                + "   type TEXT,"
                + "   headers JSONB,"
                + "   created_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
                + "   visible_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
                + "   attempts INT NOT NULL DEFAULT 0,"
                + "   picked_by TEXT"
                + " );"
                + " CREATE INDEX idx_fencepost_queue_dequeue ON fencepost_queue (queue_name, visible_at)"
            );
        }
    }

    public static void dropAll(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute(
              "DO $$ DECLARE r RECORD;"
                + " BEGIN FOR r IN SELECT sequencename FROM pg_sequences WHERE sequencename LIKE 'fencepost_st_%'"
                + " LOOP EXECUTE 'DROP SEQUENCE IF EXISTS ' || r.sequencename; END LOOP; END$$;"
                + " DROP TABLE IF EXISTS fencepost_queue;"
                + " DROP TABLE IF EXISTS fencepost_locks_tokens;"
                + " DROP TABLE IF EXISTS fencepost_locks;"
                + " DROP SEQUENCE IF EXISTS fencepost_locks_token_seq"
            );
        }
    }
}
