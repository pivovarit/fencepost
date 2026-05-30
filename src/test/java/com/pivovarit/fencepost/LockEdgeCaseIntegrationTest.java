package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.AdvisoryLock;
import com.pivovarit.fencepost.lock.FencedLock;
import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.LockFactory;
import com.pivovarit.fencepost.lock.LockNotHeldException;
import com.pivovarit.fencepost.lock.RenewableLock;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@Testcontainers
class LockEdgeCaseIntegrationTest {

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
    void createTable() throws SQLException {
        TestSchema.resetLocks(dataSource);
    }

    @Test
    void leaseAutoRenewShouldRecoverFromOneTransientFailureThenSucceed() throws Exception {
        FaultToleranceTest.FaultDataSource faultDs = new FaultToleranceTest.FaultDataSource(dataSource);
        AtomicBoolean callbackFired = new AtomicBoolean(false);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(faultDs, Duration.ofSeconds(10))
          .withAutoRenew(Duration.ofMillis(200))
          .onAutoRenewFailure(ex -> callbackFired.set(true))
          .build();

        RenewableLock lock = provider.forName("transient-1-fail");
        lock.lock();

        faultDs.failNext(1);

        Thread.sleep(2000);

        assertThat(callbackFired.get())
          .as("single transient failure should be recovered via retry, callback should not fire")
          .isFalse();

        lock.unlock();
    }

    @Test
    void leaseAutoRenewShouldFireCallbackAfterAllRetriesExhausted() throws Exception {
        FaultToleranceTest.FaultDataSource faultDs = new FaultToleranceTest.FaultDataSource(dataSource);
        CountDownLatch callbackFired = new CountDownLatch(1);
        AtomicReference<FencepostException> capturedError = new AtomicReference<>();

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(faultDs, Duration.ofSeconds(10))
          .withAutoRenew(Duration.ofMillis(200))
          .onAutoRenewFailure(ex -> {
              capturedError.set(ex);
              callbackFired.countDown();
          })
          .build();

        RenewableLock lock = provider.forName("exhaust-retries");
        lock.lock();

        faultDs.startFailing();

        assertThat(callbackFired.await(15, TimeUnit.SECONDS))
          .as("callback should fire after all 3 retries are exhausted")
          .isTrue();
        assertThat(capturedError.get()).isInstanceOf(FencepostException.class);

        faultDs.stopFailing();
        lock.close();
    }

    @Test
    void advisoryLockCloseWithoutLockShouldBeNoOp() {
        LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(dataSource).build();

        AdvisoryLock lock = provider.forName("close-no-lock");
        lock.close();
    }

    @Test
    void sessionLockCloseWithoutLockShouldBeNoOp() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("session-close-no-lock");
        lock.close();
    }

    @Test
    void leaseCloseWithoutLockShouldBeNoOp() {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(5)).build();

        RenewableLock lock = provider.forName("lease-close-no-lock");
        lock.close();
    }

    @Test
    void sessionLockShouldBeReusableAfterUnlock() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("session-reuse");
        FencingToken first = lock.lock();
        lock.unlock();

        FencingToken second = lock.lock();
        assertThat(second.value()).isGreaterThan(first.value());
        lock.unlock();
    }

    @Test
    void leaseLockShouldBeReusableAfterUnlock() {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(5)).build();

        RenewableLock lock = provider.forName("lease-reuse");
        FencingToken first = lock.lock();
        lock.unlock();

        FencingToken second = lock.lock();
        assertThat(second.value()).isGreaterThan(first.value());
        lock.unlock();
    }

    @Test
    void advisoryLockShouldBeReusableAfterUnlock() {
        LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(dataSource).build();

        AdvisoryLock lock = provider.forName("advisory-reuse");
        lock.lock();
        lock.unlock();

        lock.lock();
        lock.unlock();
    }

    @Test
    void advisoryLockWithTimeoutShouldNotLeakLockTimeoutToPool() throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(PG.getJdbcUrl());
        config.setUsername(PG.getUsername());
        config.setPassword(PG.getPassword());
        config.setMaximumPoolSize(1);
        config.setConnectionInitSql("SET lock_timeout = '11s'");

        try (HikariDataSource pool = new HikariDataSource(config)) {
            LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(pool).build();
            AdvisoryLock lock = provider.forName("advisory-timeout-leak");

            lock.lock(Duration.ofSeconds(2));
            lock.unlock();

            try (Connection conn = pool.getConnection();
                 ResultSet rs = conn.createStatement().executeQuery("SHOW lock_timeout")) {
                rs.next();
                assertThat(rs.getString(1))
                  .as("operator-configured lock_timeout must survive an advisory lock(timeout) cycle")
                  .isEqualTo("11s");
            }
        }
    }

    @Test
    void renewAfterAutoRenewFailureShouldThrowLockNotHeld() throws Exception {
        AtomicBoolean callbackFired = new AtomicBoolean(false);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(2))
          .withAutoRenew(Duration.ofMillis(100))
          .onAutoRenewFailure(ex -> callbackFired.set(true))
          .build();

        RenewableLock lock = provider.forName("renew-after-fail");
        lock.lock();

        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "UPDATE fencepost_locks SET token = token + 1 WHERE lock_name = 'renew-after-fail'");
        }

        await().atMost(Duration.ofSeconds(10)).untilTrue(callbackFired);

        assertThatThrownBy(() -> lock.renew(Duration.ofSeconds(5)))
          .isInstanceOf(LockNotHeldException.class);
    }

    @Test
    void unlockDuringConnectionFailureShouldThrow() {
        FaultToleranceTest.FaultDataSource faultDs = new FaultToleranceTest.FaultDataSource(dataSource);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(faultDs, Duration.ofSeconds(30)).build();
        RenewableLock lock = provider.forName("unlock-conn-fail");
        lock.lock();

        faultDs.startFailing();

        assertThatThrownBy(lock::unlock)
          .isInstanceOf(FencepostException.class);

        faultDs.stopFailing();
    }

    @Test
    void sessionLockConnectionDropShouldAllowReacquisitionByNewInstance() throws Exception {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock holder = provider.forName("session-drop-reacquire");
        FencingToken firstToken = holder.lock();

        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle in transaction' AND pid != pg_backend_pid()");
        }

        FencedLock newHolder = provider.forName("session-drop-reacquire");
        Optional<FencingToken> secondToken = newHolder.tryLock();
        assertThat(secondToken).isPresent();
        assertThat(secondToken.get().value()).isGreaterThan(firstToken.value());
        newHolder.unlock();
    }

    @Test
    void leaseAutoRenewThreadShouldExitCleanlyOnNormalUnlock() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(30))
          .withAutoRenew(Duration.ofMillis(100))
          .build();

        RenewableLock lock = provider.forName("clean-thread-exit");
        lock.lock();

        Thread.sleep(300);

        lock.unlock();
        Thread.sleep(200);

        long autoRenewThreads = Thread.getAllStackTraces().keySet().stream()
          .filter(t -> t.getName().contains("clean-thread-exit"))
          .filter(Thread::isAlive)
          .count();
        assertThat(autoRenewThreads)
          .as("auto-renew thread should exit cleanly after normal unlock")
          .isZero();
    }

    @Test
    void leaseWithQuietPeriodShouldPreventReacquisitionByDifferentInstance() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10))
          .withQuietPeriod(Duration.ofSeconds(2))
          .build();

        RenewableLock lock1 = provider.forName("quiet-different-instance");
        lock1.lock();
        lock1.unlock();

        LockFactory<RenewableLock> provider2 = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10)).build();
        RenewableLock lock2 = provider2.forName("quiet-different-instance");
        assertThat(lock2.tryLock())
          .as("quiet period should prevent reacquisition even from different factory")
          .isEmpty();

        Thread.sleep(2_500);

        assertThat(lock2.tryLock()).isPresent();
        lock2.unlock();
    }

    @Test
    void lockAcquisitionDuringConnectionFailureShouldThrow() {
        FaultToleranceTest.FaultDataSource faultDs = new FaultToleranceTest.FaultDataSource(dataSource);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(faultDs, Duration.ofSeconds(5)).build();
        RenewableLock lock = provider.forName("acquire-conn-fail");

        faultDs.startFailing();

        assertThatThrownBy(lock::lock)
          .isInstanceOf(FencepostException.class);

        faultDs.stopFailing();
    }

    @Test
    void tryLockDuringConnectionFailureShouldThrow() {
        FaultToleranceTest.FaultDataSource faultDs = new FaultToleranceTest.FaultDataSource(dataSource);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(faultDs, Duration.ofSeconds(5)).build();
        RenewableLock lock = provider.forName("trylock-conn-fail");

        faultDs.startFailing();

        assertThatThrownBy(lock::tryLock)
          .isInstanceOf(FencepostException.class);

        faultDs.stopFailing();
    }

    @Test
    void sessionTryLockDuringConnectionFailureShouldThrow() {
        FaultToleranceTest.FaultDataSource faultDs = new FaultToleranceTest.FaultDataSource(dataSource);

        LockFactory<FencedLock> provider = Fencepost.Locks.session(faultDs).build();
        FencedLock lock = provider.forName("session-trylock-conn-fail");

        faultDs.startFailing();

        assertThatThrownBy(lock::tryLock)
          .isInstanceOf(FencepostException.class);

        faultDs.stopFailing();
    }
}
