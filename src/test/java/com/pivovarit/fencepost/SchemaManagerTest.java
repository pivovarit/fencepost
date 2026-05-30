package com.pivovarit.fencepost;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class SchemaManagerTest {

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:17");

    static DataSource dataSource;

    @BeforeAll
    static void setupDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        dataSource = ds;
    }

    @BeforeEach
    void dropAll() throws SQLException {
        TestSchema.dropAll(dataSource);
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
                "DROP TABLE IF EXISTS custom_locks_tokens;"
                + " DROP TABLE IF EXISTS custom_locks;"
                + " DROP SEQUENCE IF EXISTS custom_locks_token_seq;"
                + " DROP TABLE IF EXISTS custom_queue;"
                + " DROP TABLE IF EXISTS mylocks_tokens;"
                + " DROP TABLE IF EXISTS mylocks;"
                + " DROP SEQUENCE IF EXISTS mylocks_token_seq;"
                + " DROP TABLE IF EXISTS myqueue;"
            );
        }
    }

    @Test
    void shouldCreateLockSchema() throws SQLException {
        SchemaManager.createLockSchema(dataSource, "fencepost_locks");

        assertThat(tableExists("fencepost_locks")).isTrue();
        assertThat(tableExists("fencepost_locks_tokens")).isTrue();
        assertThat(sequenceExists("fencepost_locks_token_seq")).isTrue();
    }

    @Test
    void shouldCreateLockSchemaIdempotently() throws SQLException {
        SchemaManager.createLockSchema(dataSource, "fencepost_locks");
        SchemaManager.createLockSchema(dataSource, "fencepost_locks");

        assertThat(tableExists("fencepost_locks")).isTrue();
    }

    @Test
    void shouldCreateLockSchemaWithCustomTableName() throws SQLException {
        SchemaManager.createLockSchema(dataSource, "custom_locks");

        assertThat(tableExists("custom_locks")).isTrue();
        assertThat(tableExists("custom_locks_tokens")).isTrue();
        assertThat(sequenceExists("custom_locks_token_seq")).isTrue();
    }

    @Test
    void shouldCreateLockSequenceWithCacheOne() throws SQLException {
        SchemaManager.createLockSchema(dataSource, "fencepost_locks");

        assertThat(sequenceCacheSize("fencepost_locks_token_seq")).isEqualTo(1L);
    }

    @Test
    void shouldCreateQueueSchema() throws SQLException {
        SchemaManager.createQueueSchema(dataSource, "fencepost_queue");

        assertThat(tableExists("fencepost_queue")).isTrue();
        assertThat(indexExists("idx_fencepost_queue_dequeue")).isTrue();
    }

    @Test
    void shouldCreateQueueSchemaWithCustomTableName() throws SQLException {
        SchemaManager.createQueueSchema(dataSource, "custom_queue");

        assertThat(tableExists("custom_queue")).isTrue();
        assertThat(indexExists("idx_custom_queue_dequeue")).isTrue();
    }

    @Test
    void shouldCreateLockSchemaConcurrentlyWithoutFailing() throws Exception {
        assertNoFailuresWhenCreatingConcurrently(() -> SchemaManager.createLockSchema(dataSource, "fencepost_locks"));

        assertThat(tableExists("fencepost_locks")).isTrue();
        assertThat(tableExists("fencepost_locks_tokens")).isTrue();
        assertThat(sequenceExists("fencepost_locks_token_seq")).isTrue();
    }

    @Test
    void shouldCreateQueueSchemaConcurrentlyWithoutFailing() throws Exception {
        assertNoFailuresWhenCreatingConcurrently(() -> SchemaManager.createQueueSchema(dataSource, "fencepost_queue"));

        assertThat(tableExists("fencepost_queue")).isTrue();
        assertThat(indexExists("idx_fencepost_queue_dequeue")).isTrue();
    }

    private static void assertNoFailuresWhenCreatingConcurrently(Runnable create) throws InterruptedException {
        int threads = 16;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            List<Throwable> failures = new CopyOnWriteArrayList<>();
            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    try {
                        start.await();
                        create.run();
                    } catch (Throwable t) {
                        failures.add(t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(failures).isEmpty();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldValidateLockSchemaWhenCorrect() throws SQLException {
        TestSchema.resetLocks(dataSource);
        SchemaManager.validateLockSchema(dataSource, "fencepost_locks");
    }

    @Test
    void shouldValidateLockSchemaWithMixedCaseTableName() throws SQLException {
        SchemaManager.createLockSchema(dataSource, "MyLocks");

        SchemaManager.validateLockSchema(dataSource, "MyLocks");
    }

    @Test
    void shouldValidateQueueSchemaWithMixedCaseTableName() throws SQLException {
        SchemaManager.createQueueSchema(dataSource, "MyQueue");

        SchemaManager.validateQueueSchema(dataSource, "MyQueue");
    }

    @Test
    void shouldFailValidationWhenLockTableMissing() {
        assertThatThrownBy(() -> SchemaManager.validateLockSchema(dataSource, "fencepost_locks"))
            .isInstanceOf(FencepostException.class)
            .hasMessageContaining("Required table 'fencepost_locks' does not exist");
    }

    @Test
    void shouldFailValidationWhenColumnMissing() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
                "CREATE TABLE fencepost_locks (lock_name TEXT PRIMARY KEY, lock_type TEXT NOT NULL);"
                + " CREATE TABLE fencepost_locks_tokens (lock_name TEXT PRIMARY KEY, token BIGINT NOT NULL DEFAULT 0);"
                + " CREATE SEQUENCE fencepost_locks_token_seq"
            );
        }

        assertThatThrownBy(() -> SchemaManager.validateLockSchema(dataSource, "fencepost_locks"))
            .isInstanceOf(FencepostException.class)
            .hasMessageContaining("missing column 'token'");
    }

    @Test
    void shouldValidateQueueSchemaWhenCorrect() throws SQLException {
        TestSchema.resetQueue(dataSource);
        SchemaManager.validateQueueSchema(dataSource, "fencepost_queue");
    }

    @Test
    void shouldFailValidationWhenQueueTableMissing() {
        assertThatThrownBy(() -> SchemaManager.validateQueueSchema(dataSource, "fencepost_queue"))
            .isInstanceOf(FencepostException.class)
            .hasMessageContaining("Required table 'fencepost_queue' does not exist");
    }

    @Test
    void shouldFailValidationWhenSequenceMissing() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
                "CREATE TABLE fencepost_locks ("
                + "  lock_name TEXT PRIMARY KEY, lock_type TEXT NOT NULL,"
                + "  token BIGINT NOT NULL DEFAULT 0, locked_by TEXT,"
                + "  locked_at TIMESTAMPTZ, expires_at TIMESTAMPTZ);"
                + " CREATE TABLE fencepost_locks_tokens ("
                + "  lock_name TEXT PRIMARY KEY, token BIGINT NOT NULL DEFAULT 0,"
                + "  last_locked_by TEXT, last_locked_at TIMESTAMPTZ)"
            );
        }

        assertThatThrownBy(() -> SchemaManager.validateLockSchema(dataSource, "fencepost_locks"))
            .isInstanceOf(FencepostException.class)
            .hasMessageContaining("Required sequence 'fencepost_locks_token_seq' does not exist");
    }

    @Test
    void shouldFailValidationWhenSequenceCacheIsNotOne() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
                "CREATE TABLE fencepost_locks ("
                + "  lock_name TEXT PRIMARY KEY, lock_type TEXT NOT NULL,"
                + "  token BIGINT NOT NULL DEFAULT 0, locked_by TEXT,"
                + "  locked_at TIMESTAMPTZ, expires_at TIMESTAMPTZ);"
                + " CREATE TABLE fencepost_locks_tokens ("
                + "  lock_name TEXT PRIMARY KEY, token BIGINT NOT NULL DEFAULT 0,"
                + "  last_locked_by TEXT, last_locked_at TIMESTAMPTZ);"
                + " CREATE SEQUENCE fencepost_locks_token_seq CACHE 50"
            );
        }

        assertThatThrownBy(() -> SchemaManager.validateLockSchema(dataSource, "fencepost_locks"))
            .isInstanceOf(FencepostException.class)
            .hasMessageContaining("has CACHE 50")
            .hasMessageContaining("CACHE 1");
    }

    private boolean tableExists(String table) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '" + table + "'");
            return rs.next();
        }
    }

    private boolean sequenceExists(String sequence) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT 1 FROM pg_sequences WHERE schemaname = 'public' AND sequencename = '" + sequence + "'");
            return rs.next();
        }
    }

    private long sequenceCacheSize(String sequence) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT cache_size FROM pg_sequences WHERE schemaname = 'public' AND sequencename = '" + sequence + "'");
            assertThat(rs.next()).isTrue();
            return rs.getLong(1);
        }
    }

    private boolean indexExists(String index) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = '" + index + "'");
            return rs.next();
        }
    }
}
