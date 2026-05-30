package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.AdvisoryLock;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class AdvisoryLockAutoCommitIntegrationTest {

    @Container
    private static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    @Test
    void lockShouldNotLeaveHolderBackendIdleInTransactionUnderAutoCommitFalse() {
        try (HikariDataSource dataSource = autoCommitFalseDataSource()) {
            AdvisoryLock lock = Fencepost.Locks.advisory(dataSource).build().forName("advisory-autocommit-lock");
            lock.lock();
            try {
                assertThat(advisoryHolderState(dataSource)).isEqualTo("idle");
            } finally {
                lock.unlock();
            }
        }
    }

    @Test
    void tryLockShouldNotLeaveHolderBackendIdleInTransactionUnderAutoCommitFalse() {
        try (HikariDataSource dataSource = autoCommitFalseDataSource()) {
            AdvisoryLock lock = Fencepost.Locks.advisory(dataSource).build().forName("advisory-autocommit-trylock");
            assertThat(lock.tryLock()).isTrue();
            try {
                assertThat(advisoryHolderState(dataSource)).isEqualTo("idle");
            } finally {
                lock.unlock();
            }
        }
    }

    private static String advisoryHolderState(HikariDataSource dataSource) {
        String query = """
            SELECT a.state FROM pg_locks l
            JOIN pg_stat_activity a ON a.pid = l.pid
            WHERE l.locktype = 'advisory' AND a.pid <> pg_backend_pid()""";
        try (Connection observer = dataSource.getConnection();
             Statement statement = observer.createStatement();
             ResultSet rs = statement.executeQuery(query)) {
            assertThat(rs.next()).as("expected exactly one advisory lock holder").isTrue();
            String state = rs.getString(1);
            assertThat(rs.next()).as("expected exactly one advisory lock holder").isFalse();
            return state;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static HikariDataSource autoCommitFalseDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setAutoCommit(false);
        config.setMaximumPoolSize(5);
        return new HikariDataSource(config);
    }
}
