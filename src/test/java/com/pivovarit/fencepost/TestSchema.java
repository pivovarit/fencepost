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
                + " LOOP EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(r.sequencename); END LOOP; END$$;"
                + " DROP TABLE IF EXISTS fencepost_locks_tokens;"
                + " DROP TABLE IF EXISTS fencepost_locks;"
                + " DROP SEQUENCE IF EXISTS fencepost_locks_token_seq"
            );
        }
        SchemaManager.createLockSchema(ds, "fencepost_locks");
    }

    public static void resetQueue(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute("DROP TABLE IF EXISTS fencepost_queue");
        }
        SchemaManager.createQueueSchema(ds, "fencepost_queue");
    }

    public static void dropAll(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute(
              "DO $$ DECLARE r RECORD;"
                + " BEGIN FOR r IN SELECT sequencename FROM pg_sequences WHERE sequencename LIKE 'fencepost_st_%'"
                + " LOOP EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(r.sequencename); END LOOP; END$$;"
                + " DROP TABLE IF EXISTS fencepost_queue;"
                + " DROP TABLE IF EXISTS fencepost_locks_tokens;"
                + " DROP TABLE IF EXISTS fencepost_locks;"
                + " DROP SEQUENCE IF EXISTS fencepost_locks_token_seq"
            );
        }
    }
}
