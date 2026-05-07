package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.AdvisoryLock;
import com.pivovarit.fencepost.lock.FencedLock;
import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.LockFactory;
import com.pivovarit.fencepost.lock.RenewableLock;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LockStressTest {

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:17");

    static HikariDataSource dataSource;

    @BeforeAll
    static void setupDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(PG.getJdbcUrl());
        config.setUsername(PG.getUsername());
        config.setPassword(PG.getPassword());
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(5000);
        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void tearDown() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @BeforeEach
    void createTable() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement()
              .execute("DROP TABLE IF EXISTS fencepost_locks_tokens; DROP TABLE IF EXISTS fencepost_locks; " +
                "CREATE TABLE fencepost_locks (  lock_name TEXT PRIMARY KEY,  lock_type TEXT NOT NULL,  token BIGINT NOT NULL DEFAULT 0,  locked_by TEXT,  locked_at TIMESTAMP WITH TIME ZONE,  expires_at TIMESTAMP WITH TIME ZONE); " +
                "CREATE TABLE fencepost_locks_tokens (  lock_name TEXT PRIMARY KEY,  token BIGINT NOT NULL DEFAULT 0,  last_locked_by TEXT,  last_locked_at TIMESTAMP WITH TIME ZONE)");
        }
    }

    @Test
    @Order(1)
    void sessionLockMutualExclusionUnderContention() throws Exception {
        int threads = 10;
        int iterationsPerThread = 50;
        int totalIterations = threads * iterationsPerThread;

        LockFactory<FencedLock> factory = Fencepost.Locks.session(dataSource).build();
        AtomicBoolean inside = new AtomicBoolean(false);
        AtomicInteger overlaps = new AtomicInteger(0);
        List<Long> tokens = new CopyOnWriteArrayList<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        while (true) {
                            try {
                                FencedLock lock = factory.forName("stress-session");
                                Optional<FencingToken> maybeToken = lock.tryLock();
                                if (maybeToken.isPresent()) {
                                    try {
                                        if (!inside.compareAndSet(false, true)) {
                                            overlaps.incrementAndGet();
                                        }
                                        Thread.sleep(1);
                                        inside.set(false);
                                        tokens.add(maybeToken.get().value());
                                    } finally {
                                        lock.unlock();
                                    }
                                    break;
                                }
                            } catch (FencepostException e) {
                                // pool exhaustion under contention - retry
                            }
                            Thread.yield();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(120, TimeUnit.SECONDS))
          .as("all threads should finish within 120s")
          .isTrue();
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertThat(overlaps.get()).as("no overlapping critical sections").isZero();
        assertThat(tokens).hasSize(totalIterations);
        assertThat(tokens).doesNotHaveDuplicates();
    }

    @Test
    @Order(2)
    void leaseLockMutualExclusionUnderContention() throws Exception {
        int threads = 10;
        int iterationsPerThread = 50;
        int totalIterations = threads * iterationsPerThread;

        LockFactory<RenewableLock> factory = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(30)).build();
        AtomicBoolean inside = new AtomicBoolean(false);
        AtomicInteger overlaps = new AtomicInteger(0);
        List<Long> tokens = new CopyOnWriteArrayList<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        RenewableLock lock = factory.forName("stress-lease");
                        FencingToken token = lock.lock();
                        try {
                            if (!inside.compareAndSet(false, true)) {
                                overlaps.incrementAndGet();
                            }
                            Thread.sleep(1);
                            inside.set(false);
                            tokens.add(token.value());
                        } finally {
                            lock.unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(120, TimeUnit.SECONDS))
          .as("all threads should finish within 120s")
          .isTrue();
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertThat(overlaps.get()).as("no overlapping critical sections").isZero();
        assertThat(tokens).hasSize(totalIterations);
        assertThat(tokens).doesNotHaveDuplicates();
    }

    @Test
    @Order(3)
    void advisoryLockMutualExclusionUnderContention() throws Exception {
        int threads = 10;
        int iterationsPerThread = 50;

        LockFactory<AdvisoryLock> factory = Fencepost.Locks.advisory(dataSource).build();
        AtomicBoolean inside = new AtomicBoolean(false);
        AtomicInteger overlaps = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        AdvisoryLock lock = factory.forName("stress-advisory");
                        lock.lock();
                        try {
                            if (!inside.compareAndSet(false, true)) {
                                overlaps.incrementAndGet();
                            }
                            Thread.sleep(1);
                            inside.set(false);
                        } finally {
                            lock.unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(120, TimeUnit.SECONDS))
          .as("all threads should finish within 120s")
          .isTrue();
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertThat(overlaps.get()).as("no overlapping critical sections").isZero();
    }

    @Test
    @Order(4)
    void connectionPoolResilienceUnderRapidLockCycling() throws Exception {
        int threads = 20;
        int iterationsPerThread = 100;
        int totalCycles = threads * iterationsPerThread;

        LockFactory<RenewableLock> factory = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(30)).build();
        List<Exception> exceptions = new CopyOnWriteArrayList<>();
        AtomicInteger completedCycles = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        try {
                            RenewableLock lock = factory.forName("stress-pool");
                            lock.lock();
                            lock.unlock();
                            completedCycles.incrementAndGet();
                        } catch (Exception e) {
                            exceptions.add(e);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(60, TimeUnit.SECONDS))
          .as("all %d cycles should complete within 60s", totalCycles)
          .isTrue();
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertThat(exceptions)
          .as("no exceptions during rapid lock cycling")
          .isEmpty();
        assertThat(completedCycles.get()).isEqualTo(totalCycles);

        await().atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> assertThat(dataSource.getHikariPoolMXBean().getActiveConnections())
            .as("all connections should be returned to the pool")
            .isZero());
    }

    @Test
    @Order(5)
    void lockRecoveryAfterBackendTermination() throws Exception {
        LockFactory<FencedLock> factory = Fencepost.Locks.session(dataSource).build();

        FencedLock holder = factory.forName("stress-terminate");
        FencingToken firstToken = holder.lock();

        // Kill the backend holding the lock
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
               "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
                 "WHERE state = 'idle in transaction' AND pid != pg_backend_pid()")) {
            ps.executeQuery();
        }

        // Another attempt should succeed with a higher token
        FencedLock contender = factory.forName("stress-terminate");
        Optional<FencingToken> secondToken = contender.tryLock();

        assertThat(secondToken)
          .as("lock should be acquirable after backend termination")
          .isPresent();
        assertThat(secondToken.get().value())
          .as("new token should be higher than the killed session's token")
          .isGreaterThan(firstToken.value());

        contender.unlock();
    }
}
