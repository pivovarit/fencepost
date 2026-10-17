package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.LockFactory;
import com.pivovarit.fencepost.lock.RenewableLock;
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
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class LeaseAcquisitionTimestampIntegrationTest {

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
    void createTables() throws SQLException {
        TestSchema.resetLocks(dataSource);
    }

    @Test
    void acquisitionShouldNotReturnAnAlreadyExpiredLeaseWhenBlockedBehindATableLock() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofMillis(500)).build();
        RenewableLock holder = provider.forName("blocked-behind-table-lock");

        holder.tryLock().orElseThrow();
        holder.unlock();

        Connection blocker = dataSource.getConnection();
        blocker.setAutoCommit(false);
        blocker.createStatement().execute("LOCK TABLE fencepost_locks IN ACCESS EXCLUSIVE MODE");

        ScheduledExecutorService releaser = new ScheduledThreadPoolExecutor(1);
        releaser.schedule(() -> {
            try {
                blocker.commit();
                blocker.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, 800, TimeUnit.MILLISECONDS);

        Instant beforeAcquire = Instant.now();
        try {
            holder.lock();
            Instant acquiredAt = Instant.now();
            assertThat(Duration.between(beforeAcquire, acquiredAt))
              .as("acquisition must have actually waited out the table lock")
              .isGreaterThanOrEqualTo(Duration.ofMillis(700));

            RenewableLock contender = provider.forName("blocked-behind-table-lock");
            assertThat(contender.tryLock())
              .as("a lease granted right as the blocking table lock released must not already be expired")
              .isEmpty();
        } finally {
            releaser.shutdown();
            holder.close();
        }
    }
}
