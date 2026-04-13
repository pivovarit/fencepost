package com.pivovarit.fencepost.dashboard;

import com.pivovarit.fencepost.DashboardApi;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class DashboardApiTest {

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:17");

    static DataSource dataSource;

    static final String LOCKS_TABLE = "fencepost_locks";
    static final String QUEUE_TABLE = "fencepost_queue";

    @BeforeAll
    static void setupDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        dataSource = ds;
    }

    @BeforeEach
    void dropTables() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "DROP TABLE IF EXISTS fencepost_queue; DROP TABLE IF EXISTS fencepost_locks;"
            );
        }
    }

    void createLocksTable() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "CREATE TABLE fencepost_locks (" +
              "  lock_name TEXT PRIMARY KEY," +
              "  token BIGINT NOT NULL DEFAULT 0," +
              "  locked_by TEXT," +
              "  locked_at TIMESTAMPTZ," +
              "  expires_at TIMESTAMPTZ" +
              ")"
            );
        }
    }

    void createQueueTable() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "CREATE TABLE fencepost_queue (" +
              "  id BIGSERIAL PRIMARY KEY," +
              "  queue_name TEXT NOT NULL," +
              "  payload BYTEA NOT NULL," +
              "  type TEXT," +
              "  headers JSONB," +
              "  created_at TIMESTAMPTZ NOT NULL DEFAULT now()," +
              "  visible_at TIMESTAMPTZ NOT NULL DEFAULT now()," +
              "  attempts INT NOT NULL DEFAULT 0," +
              "  picked_by TEXT" +
              ")"
            );
        }
    }

    DashboardApi api() {
        return new DashboardApi(dataSource, LOCKS_TABLE, QUEUE_TABLE);
    }

    // --- status() tests ---

    @Test
    void shouldDetectNoTablesExist() throws Exception {
        String json = api().status();

        assertThat(json).contains("\"locks_enabled\":false");
        assertThat(json).contains("\"queues_enabled\":false");
    }

    @Test
    void shouldDetectLocksTableExists() throws Exception {
        createLocksTable();

        String json = api().status();

        assertThat(json).contains("\"locks_enabled\":true");
        assertThat(json).contains("\"queues_enabled\":false");
    }

    @Test
    void shouldDetectQueueTableExists() throws Exception {
        createQueueTable();

        String json = api().status();

        assertThat(json).contains("\"locks_enabled\":false");
        assertThat(json).contains("\"queues_enabled\":true");
    }

    // --- locks() tests ---

    @Test
    void shouldReturnEmptyLocksArray() throws Exception {
        createLocksTable();

        String json = api().locks();

        assertThat(json).isEqualTo("[]");
    }

    @Test
    void shouldReturnLockData() throws Exception {
        createLocksTable();
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "INSERT INTO fencepost_locks (lock_name, token, locked_by, locked_at, expires_at) " +
              "VALUES ('my-lock', 42, 'worker-1', now(), now() + interval '1 hour')"
            );
        }

        String json = api().locks();

        assertThat(json).contains("\"name\":\"my-lock\"");
        assertThat(json).contains("\"token\":42");
        assertThat(json).contains("\"locked_by\":\"worker-1\"");
        assertThat(json).contains("\"is_held\":true");
    }

    @Test
    void shouldReturnSingleLock() throws Exception {
        createLocksTable();
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "INSERT INTO fencepost_locks (lock_name, token) VALUES ('lock-a', 1), ('lock-b', 2)"
            );
        }

        String json = api().lock("lock-b");

        assertThat(json).contains("\"name\":\"lock-b\"");
        assertThat(json).doesNotContain("\"name\":\"lock-a\"");
    }

    @Test
    void shouldReturnNullForMissingLock() throws Exception {
        createLocksTable();

        String json = api().lock("nonexistent");

        assertThat(json).isEqualTo("null");
    }

    // --- queues() tests ---

    @Test
    void shouldReturnEmptyQueuesArray() throws Exception {
        createQueueTable();

        String json = api().queues();

        assertThat(json).isEqualTo("[]");
    }

    @Test
    void shouldReturnQueueSummary() throws Exception {
        createQueueTable();
        try (Connection conn = dataSource.getConnection()) {
            // 2 visible messages, 1 in-flight
            conn.createStatement().execute(
              "INSERT INTO fencepost_queue (queue_name, payload, visible_at, picked_by) VALUES " +
              "('my-queue', 'msg1'::bytea, now() - interval '1 second', NULL)," +
              "('my-queue', 'msg2'::bytea, now() - interval '2 seconds', NULL)," +
              "('my-queue', 'msg3'::bytea, now() - interval '3 seconds', 'worker-1')"
            );
        }

        String json = api().queues();

        assertThat(json).contains("\"name\":\"my-queue\"");
        assertThat(json).contains("\"total\":3");
        assertThat(json).contains("\"visible\":2");
        assertThat(json).contains("\"in_flight\":1");
    }

    @Test
    void shouldReturnQueueDetailWithInFlightMessages() throws Exception {
        createQueueTable();
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "INSERT INTO fencepost_queue (queue_name, payload, picked_by, attempts) VALUES " +
              "('detail-queue', 'hello world'::bytea, 'worker-99', 2)"
            );
        }

        String json = api().queue("detail-queue");

        assertThat(json).contains("\"name\":\"detail-queue\"");
        assertThat(json).contains("\"in_flight\":1");
        assertThat(json).contains("\"payload_preview\":\"aGVsbG8gd29ybGQ=\"");
        assertThat(json).contains("\"picked_by\":\"worker-99\"");
        assertThat(json).contains("\"attempts\":2");
    }
}
