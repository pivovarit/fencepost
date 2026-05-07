package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.AdvisoryLock;
import com.pivovarit.fencepost.lock.FencedLock;
import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.LockAcquisitionTimeoutException;
import com.pivovarit.fencepost.lock.LockFactory;
import com.pivovarit.fencepost.lock.LockNotHeldException;
import com.pivovarit.fencepost.lock.RenewableLock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@Testcontainers
class FencepostLockIntegrationTest {

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
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement()
              .execute("DROP TABLE IF EXISTS fencepost_locks_tokens; DROP TABLE IF EXISTS fencepost_locks; " +
                "CREATE TABLE fencepost_locks (  lock_name TEXT PRIMARY KEY,  lock_type TEXT NOT NULL,  token BIGINT NOT NULL DEFAULT 0,  locked_by TEXT,  locked_at TIMESTAMP WITH TIME ZONE,  expires_at TIMESTAMP WITH TIME ZONE); " +
                "CREATE TABLE fencepost_locks_tokens (  lock_name TEXT PRIMARY KEY,  token BIGINT NOT NULL DEFAULT 0,  last_locked_by TEXT,  last_locked_at TIMESTAMP WITH TIME ZONE)");
        }
    }

    @Test
    void shouldAcquireAndReleaseLock() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("test-lock");
        FencingToken token = lock.lock();

        assertThat(token).isNotNull();
        assertThat(token.value()).isGreaterThan(0);

        lock.unlock();
    }

    @Test
    void shouldClearMetadataOnSessionLockUnlock() throws Exception {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("session-meta-test");
        lock.lock();
        lock.unlock();

        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.createStatement()
               .executeQuery("SELECT locked_by, locked_at FROM fencepost_locks WHERE lock_name = 'session-meta-test'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("locked_by")).isNull();
            assertThat(rs.getTimestamp("locked_at")).isNull();
        }
    }

    @Test
    void sessionLockMetadataShouldBeVisibleToOtherConnections() throws Exception {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("session-visible-meta-test");
        lock.lock();

        try {
            try (Connection conn = dataSource.getConnection();
                 ResultSet rs = conn.createStatement()
                   .executeQuery("SELECT COALESCE(t.last_locked_by, l.locked_by) AS last_locked_by, " +
                     "COALESCE(t.last_locked_at, l.locked_at) AS last_locked_at " +
                     "FROM fencepost_locks l LEFT JOIN fencepost_locks_tokens t ON l.lock_name = t.lock_name " +
                     "WHERE l.lock_name = 'session-visible-meta-test'")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("last_locked_by"))
                  .as("last_locked_by should be visible to other connections while the session lock is held")
                  .isNotNull();
                assertThat(rs.getTimestamp("last_locked_at"))
                  .as("last_locked_at should be visible to other connections while the session lock is held")
                  .isNotNull();
            }
        } finally {
            lock.unlock();
        }
    }

    @Test
    void shouldReturnStrictlyIncreasingTokens() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        long previousValue = 0;
        for (int i = 0; i < 10; i++) {
            FencedLock lock = provider.forName("token-test");
            FencingToken token = lock.lock();
            assertThat(token.value()).isGreaterThan(previousValue);
            previousValue = token.value();
            lock.unlock();
        }
    }

    @Test
    void tryLockShouldReturnEmptyWhenHeld() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock holder = provider.forName("contended-lock");
        holder.lock();

        try {
            FencedLock contender = provider.forName("contended-lock");
            Optional<FencingToken> result = contender.tryLock();
            assertThat(result).isEmpty();
        } finally {
            holder.unlock();
        }
    }

    @Test
    void lockShouldBlockUntilReleased() throws Exception {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock holder = provider.forName("blocking-test");
        FencingToken firstToken = holder.lock();

        CountDownLatch acquired = new CountDownLatch(1);
        AtomicReference<FencingToken> secondToken = new AtomicReference<>();

        Thread contender = new Thread(() -> {
            FencedLock lock = provider.forName("blocking-test");
            secondToken.set(lock.lock());
            acquired.countDown();
            lock.unlock();
        });
        contender.start();

        Thread.sleep(200);
        assertThat(acquired.getCount()).isEqualTo(1);

        holder.unlock();
        assertThat(acquired.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(secondToken.get().value()).isGreaterThan(firstToken.value());
    }

    @Test
    void lockWithTimeoutShouldThrowOnTimeout() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock holder = provider.forName("timeout-test");
        holder.lock();

        try {
            FencedLock contender = provider.forName("timeout-test");
            assertThatThrownBy(() -> contender.lock(Duration.ofMillis(500)))
              .isInstanceOf(LockAcquisitionTimeoutException.class);
        } finally {
            holder.unlock();
        }
    }

    @Test
    void unlockWhenNotHeldShouldThrow() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("not-held");
        assertThatThrownBy(lock::unlock)
          .isInstanceOf(LockNotHeldException.class);
    }

    @Test
    void tryWithResourcesShouldReleaseLock() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencingToken firstToken;
        try (FencedLock lock = provider.forName("auto-close")) {
            firstToken = lock.lock();
        }

        FencedLock second = provider.forName("auto-close");
        Optional<FencingToken> secondToken = second.tryLock();
        assertThat(secondToken).isPresent();
        assertThat(secondToken.get().value()).isGreaterThan(firstToken.value());
        second.unlock();
    }

    @Test
    void concurrentLocksShouldProduceOrderedTokens() throws Exception {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        List<Long> tokens = new CopyOnWriteArrayList<>();
        int iterations = 10;
        CountDownLatch done = new CountDownLatch(2);

        Runnable worker = () -> {
            for (int i = 0; i < iterations; i++) {
                FencedLock lock = provider.forName("contention-test");
                FencingToken token = lock.lock();
                tokens.add(token.value());
                lock.unlock();
            }
            done.countDown();
        };

        Thread t1 = new Thread(worker);
        Thread t2 = new Thread(worker);
        t1.start();
        t2.start();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();

        assertThat(tokens).hasSize(20);
        assertThat(tokens).doesNotHaveDuplicates();
    }

    @Test
    void withLockShouldAcquireRunAndRelease() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("withlock-test");
        AtomicReference<FencingToken> capturedToken = new AtomicReference<>();

        lock.runLocked(capturedToken::set);

        assertThat(capturedToken.get()).isNotNull();
        assertThat(capturedToken.get().value()).isGreaterThan(0);

        FencedLock second = provider.forName("withlock-test");
        Optional<FencingToken> secondToken = second.tryLock();
        assertThat(secondToken).isPresent();
        second.unlock();
    }

    @Test
    void withLockWithTimeoutShouldAcquireRunAndRelease() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("withlock-timeout-test");
        AtomicReference<FencingToken> capturedToken = new AtomicReference<>();

        lock.runLocked(Duration.ofSeconds(5), capturedToken::set);

        assertThat(capturedToken.get()).isNotNull();
        assertThat(capturedToken.get().value()).isGreaterThan(0);

        FencedLock second = provider.forName("withlock-timeout-test");
        Optional<FencingToken> secondToken = second.tryLock();
        assertThat(secondToken).isPresent();
        second.unlock();
    }

    @Test
    void withLockShouldPropagateUncheckedExceptionAndRelease() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("withlock-unchecked-test");

        assertThatThrownBy(() -> lock.runLocked(token -> {
            throw new IllegalArgumentException("boom");
        }))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("boom");

        FencedLock second = provider.forName("withlock-unchecked-test");
        Optional<FencingToken> secondToken = second.tryLock();
        assertThat(secondToken).isPresent();
        second.unlock();
    }

    @Test
    void withLockShouldWrapCheckedExceptionAndRelease() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("withlock-checked-test");

        assertThatThrownBy(() -> lock.runLocked(token -> {
            throw new IOException("disk full");
        }))
          .isInstanceOf(FencepostException.class)
          .hasCauseInstanceOf(IOException.class);

        FencedLock second = provider.forName("withlock-checked-test");
        Optional<FencingToken> secondToken = second.tryLock();
        assertThat(secondToken).isPresent();
        second.unlock();
    }

    @Test
    void withLockWhileHeldShouldThrow() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("withlock-guard-test");
        lock.lock();

        try {
            assertThatThrownBy(() -> lock.runLocked(token -> {}))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("withlock-guard-test");
        } finally {
            lock.unlock();
        }
    }

    @Test
    void ttlExpiredLockShouldBeAcquirable() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(1)).build();

        RenewableLock holder = provider.forName("ttl-test");
        FencingToken firstToken = holder.lock();
        holder.unlock();

        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "UPDATE fencepost_locks SET expires_at = now() - interval '1 second' WHERE lock_name = 'ttl-test'"
            );
        }

        RenewableLock second = provider.forName("ttl-test");
        FencingToken secondToken = second.lock();
        assertThat(secondToken.value()).isGreaterThan(firstToken.value());
        second.unlock();
    }

    @Test
    void autoRenewShouldExtendExpiry() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(2))
          .withAutoRenew(Duration.ofSeconds(1))
          .build();

        RenewableLock lock = provider.forName("heartbeat-test");
        lock.lock();

        long initialExpiresAt = getExpiresAtEpoch("heartbeat-test");

        Thread.sleep(1500);

        long updatedExpiresAt = getExpiresAtEpoch("heartbeat-test");
        assertThat(updatedExpiresAt).isGreaterThan(initialExpiresAt);

        lock.unlock();
    }

    @Test
    void autoRenewShouldFailAfterLeaseExpired() throws Exception {
        AtomicBoolean callbackFired = new AtomicBoolean(false);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10))
          .withAutoRenew(Duration.ofMillis(100))
          .onAutoRenewFailure(ex -> callbackFired.set(true))
          .build();

        RenewableLock lock = provider.forName("expired-auto-renew-test");
        lock.lock();

        expireLease("expired-auto-renew-test");

        await().atMost(Duration.ofSeconds(5)).untilTrue(callbackFired);

        assertThat(isExpired("expired-auto-renew-test")).isTrue();
        assertThatThrownBy(lock::unlock)
          .isInstanceOf(LockNotHeldException.class);
    }

    @Test
    void renewShouldExtendExpiry() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(2)).build();

        RenewableLock lock = provider.forName("renew-test");
        lock.lock();

        long initialExpiry = getExpiresAtEpoch("renew-test");

        lock.renew(Duration.ofSeconds(30));

        long renewedExpiry = getExpiresAtEpoch("renew-test");
        assertThat(renewedExpiry).isGreaterThan(initialExpiry);

        lock.unlock();
    }

    @Test
    void renewShouldFailAfterLeaseExpired() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10)).build();

        RenewableLock lock = provider.forName("expired-renew-test");
        lock.lock();

        expireLease("expired-renew-test");

        assertThatThrownBy(() -> lock.renew(Duration.ofSeconds(30)))
          .isInstanceOf(LockNotHeldException.class);
        assertThat(isExpired("expired-renew-test")).isTrue();
    }

    @Test
    void renewShouldUpdateAutoRenewWindow() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(2))
          .withAutoRenew(Duration.ofSeconds(1))
          .build();

        RenewableLock lock = provider.forName("renew-heartbeat-test");
        lock.lock();

        lock.renew(Duration.ofSeconds(60));

        Thread.sleep(1_500);

        long expiry = getExpiresAtEpoch("renew-heartbeat-test");
        long now = System.currentTimeMillis() / 1000;
        assertThat(expiry - now).isGreaterThan(50);

        lock.unlock();
    }

    @Test
    void renewWhenNotHeldShouldThrow() {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(2)).build();

        RenewableLock lock = provider.forName("renew-not-held");
        assertThatThrownBy(() -> lock.renew(Duration.ofSeconds(5)))
          .isInstanceOf(LockNotHeldException.class);
    }

    @Test
    void renewWithZeroDurationShouldThrow() {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(2)).build();

        RenewableLock lock = provider.forName("renew-zero");
        lock.lock();

        try {
            assertThatThrownBy(() -> lock.renew(Duration.ZERO))
              .isInstanceOf(IllegalArgumentException.class);
        } finally {
            lock.unlock();
        }
    }

    @Test
    void renewWithNegativeDurationShouldThrow() {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(2)).build();

        RenewableLock lock = provider.forName("renew-negative");
        lock.lock();

        try {
            assertThatThrownBy(() -> lock.renew(Duration.ofSeconds(-1)))
              .isInstanceOf(IllegalArgumentException.class);
        } finally {
            lock.unlock();
        }
    }

    @Test
    void isSupersededShouldReturnFalseForCurrentToken() {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10)).build();

        RenewableLock lock = provider.forName("superseded-test");
        FencingToken token = lock.lock();

        assertThat(lock.isSuperseded(token)).isFalse();

        lock.unlock();
    }

    @Test
    void isSupersededShouldReturnTrueForOldToken() {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10)).build();

        RenewableLock lock1 = provider.forName("superseded-test-2");
        FencingToken oldToken = lock1.lock();
        lock1.unlock();

        RenewableLock lock2 = provider.forName("superseded-test-2");
        lock2.lock();

        assertThat(lock2.isSuperseded(oldToken)).isTrue();

        lock2.unlock();
    }

    @Test
    void isSupersededShouldThrowWhenRowMissing() {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10)).build();

        RenewableLock lock = provider.forName("nonexistent-lock");

        assertThatThrownBy(() -> lock.isSuperseded(new FencingToken(1)))
          .isInstanceOf(FencepostException.class)
          .hasMessageContaining("not found");
    }

    @Test
    void quietPeriodShouldPreventImmediateReacquisition() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10))
          .withQuietPeriod(Duration.ofSeconds(3))
          .build();

        RenewableLock lock = provider.forName("quiet-test");
        lock.lock();
        lock.unlock();

        RenewableLock lock2 = provider.forName("quiet-test");
        assertThat(lock2.tryLock()).isEmpty();

        Thread.sleep(3_500);

        RenewableLock lock3 = provider.forName("quiet-test");
        assertThat(lock3.tryLock()).isPresent();
        lock3.unlock();
    }

    @Test
    void quietPeriodShouldApplyWhenLockHeldLongerThanQuietPeriod() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(30))
          .withQuietPeriod(Duration.ofSeconds(2))
          .build();

        RenewableLock lock = provider.forName("quiet-held-long-test");
        lock.lock();
        Thread.sleep(3_000);
        lock.unlock();

        RenewableLock lock2 = provider.forName("quiet-held-long-test");
        assertThat(lock2.tryLock()).isEmpty();

        Thread.sleep(2_500);

        RenewableLock lock3 = provider.forName("quiet-held-long-test");
        assertThat(lock3.tryLock()).isPresent();
        lock3.unlock();
    }

    @Test
    void quietPeriodShouldClearLockedAtOnUnlock() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10))
          .withQuietPeriod(Duration.ofSeconds(3))
          .build();

        RenewableLock lock = provider.forName("quiet-clear-test");
        lock.lock();
        lock.unlock();

        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
               "SELECT locked_by, locked_at, expires_at FROM fencepost_locks WHERE lock_name = 'quiet-clear-test'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("locked_by")).isNotNull();
            assertThat(rs.getTimestamp("locked_at")).isNull();
            assertThat(rs.getTimestamp("expires_at")).isNotNull();
        }
    }

    @Test
    void unlockShouldFailAfterLeaseExpiredAndNotApplyQuietPeriod() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10))
          .withQuietPeriod(Duration.ofSeconds(30))
          .build();

        RenewableLock lock = provider.forName("expired-unlock-test");
        lock.lock();

        expireLease("expired-unlock-test");

        assertThatThrownBy(lock::unlock)
          .isInstanceOf(LockNotHeldException.class);

        RenewableLock next = provider.forName("expired-unlock-test");
        assertThat(next.tryLock()).isPresent();
        next.unlock();
    }

    @Test
    void unlockShouldIncrementTokenToPreventGhostAutoRenew() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(30))
          .withAutoRenew(Duration.ofSeconds(1))
          .withQuietPeriod(Duration.ofSeconds(2))
          .build();

        RenewableLock lock = provider.forName("ghost-renew-test");
        lock.lock();

        long tokenBeforeUnlock;
        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
               "SELECT token FROM fencepost_locks WHERE lock_name = 'ghost-renew-test'")) {
            assertThat(rs.next()).isTrue();
            tokenBeforeUnlock = rs.getLong(1);
        }

        lock.unlock();

        Thread.sleep(2_000);

        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.createStatement().executeQuery(
               "SELECT token, extract(epoch from expires_at) FROM fencepost_locks WHERE lock_name = 'ghost-renew-test'")) {
            assertThat(rs.next()).isTrue();
            long tokenAfterUnlock = rs.getLong(1);
            long expiresAtEpoch = rs.getLong(2);
            long now = System.currentTimeMillis() / 1000;

            assertThat(tokenAfterUnlock)
              .as("unlock should increment token to invalidate ghost auto-renew")
              .isGreaterThan(tokenBeforeUnlock);
            assertThat(expiresAtEpoch - now)
              .as("expires_at should not be extended beyond the quiet period by ghost auto-renew")
              .isLessThanOrEqualTo(2);
        }
    }

    @Test
    void onAutoRenewFailureShouldBeCalledWhenLockIsStolen() throws Exception {
        AtomicBoolean callbackFired = new AtomicBoolean(false);
        AtomicReference<FencepostException> callbackError = new AtomicReference<>();

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(2))
          .withAutoRenew(Duration.ofSeconds(1))
          .onAutoRenewFailure(ex -> {
              callbackFired.set(true);
              callbackError.set(ex);
          })
          .build();

        RenewableLock lock = provider.forName("heartbeat-callback-test");
        lock.lock();

        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "UPDATE fencepost_locks SET token = token + 1 WHERE lock_name = 'heartbeat-callback-test'"
            );
        }

        await().atMost(Duration.ofSeconds(10)).untilTrue(callbackFired);

        assertThat(callbackError.get())
          .isInstanceOf(FencepostException.class)
          .hasMessageContaining("heartbeat-callback-test");
    }

    @Test
    void autoRenewFailureShouldInvalidateCurrentToken() throws Exception {
        AtomicBoolean callbackFired = new AtomicBoolean(false);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(2))
          .withAutoRenew(Duration.ofSeconds(1))
          .onAutoRenewFailure(ex -> callbackFired.set(true))
          .build();

        RenewableLock lock = provider.forName("heartbeat-invalidate-test");
        lock.lock();

        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(
              "UPDATE fencepost_locks SET token = token + 1 WHERE lock_name = 'heartbeat-invalidate-test'"
            );
        }

        await().atMost(Duration.ofSeconds(10)).untilTrue(callbackFired);

        assertThatThrownBy(lock::unlock)
          .isInstanceOf(LockNotHeldException.class);
    }

    @Test
    void unlockShouldStillBePossibleAfterAutoRenewFailure() throws Exception {
        AtomicBoolean callbackFired = new AtomicBoolean(false);
        AtomicReference<Long> acquiredToken = new AtomicReference<>();

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(5))
          .withAutoRenew(Duration.ofSeconds(1))
          .onAutoRenewFailure(ex -> callbackFired.set(true))
          .build();

        RenewableLock lock = provider.forName("heartbeat-unlock-test");
        FencingToken token = lock.lock();
        acquiredToken.set(token.value());

        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement()
              .execute("UPDATE fencepost_locks SET token = token + 1 WHERE lock_name = 'heartbeat-unlock-test'");
        }

        await().atMost(Duration.ofSeconds(10)).untilTrue(callbackFired);

        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement("UPDATE fencepost_locks SET token = ? WHERE lock_name = 'heartbeat-unlock-test'")) {
                ps.setLong(1, acquiredToken.get());
                ps.executeUpdate();
            }
        }

        lock.unlock();

        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.createStatement()
               .executeQuery("SELECT locked_by FROM fencepost_locks WHERE lock_name = 'heartbeat-unlock-test'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("locked_by")).isNull();
        }
    }

    @Test
    void autoRenewFailureShouldCleanUpThreadState() throws Exception {
        AtomicBoolean callbackFired = new AtomicBoolean(false);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10))
          .withAutoRenew(Duration.ofMillis(100))
          .onAutoRenewFailure(ex -> callbackFired.set(true))
          .build();

        RenewableLock lock = provider.forName("auto-renew-cleanup-test");
        lock.lock();

        expireLease("auto-renew-cleanup-test");

        await().atMost(Duration.ofSeconds(5)).untilTrue(callbackFired);

        Thread.sleep(200);

        long autoRenewThreads = Thread.getAllStackTraces().keySet().stream()
          .filter(t -> t.getName().contains("auto-renew-cleanup-test"))
          .filter(Thread::isAlive)
          .count();
        assertThat(autoRenewThreads)
          .as("auto-renew thread should have exited after failure")
          .isZero();

        assertThatThrownBy(lock::unlock)
          .isInstanceOf(LockNotHeldException.class);
    }

    @Test
    void unlockShouldStopAutoRenewThreadAfterManualRenewInvalidatesToken() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(60))
          .withAutoRenew(Duration.ofSeconds(30))
          .build();

        RenewableLock lock = provider.forName("unlock-renew-leak-test");
        lock.lock();

        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement()
              .execute("UPDATE fencepost_locks SET token = token + 1 WHERE lock_name = 'unlock-renew-leak-test'");
        }

        assertThatThrownBy(() -> lock.renew(Duration.ofSeconds(5)))
          .isInstanceOf(LockNotHeldException.class);

        assertThatThrownBy(lock::unlock)
          .isInstanceOf(LockNotHeldException.class);

        long autoRenewThreads = Thread.getAllStackTraces().keySet().stream()
          .filter(t -> t.getName().contains("unlock-renew-leak-test"))
          .filter(Thread::isAlive)
          .count();
        assertThat(autoRenewThreads)
          .as("auto-renew thread should be stopped by unlock() even after renew() invalidated the token")
          .isZero();
    }

    @Test
    void closeShouldStopAutoRenewThreadAfterManualRenewInvalidatesToken() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(60))
          .withAutoRenew(Duration.ofSeconds(30))
          .build();

        RenewableLock lock = provider.forName("close-renew-leak-test");
        lock.lock();

        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement()
              .execute("UPDATE fencepost_locks SET token = token + 1 WHERE lock_name = 'close-renew-leak-test'");
        }

        assertThatThrownBy(() -> lock.renew(Duration.ofSeconds(5)))
          .isInstanceOf(LockNotHeldException.class);

        lock.close();

        long autoRenewThreads = Thread.getAllStackTraces().keySet().stream()
          .filter(t -> t.getName().contains("close-renew-leak-test"))
          .filter(Thread::isAlive)
          .count();
        assertThat(autoRenewThreads)
          .as("auto-renew thread should be stopped by close() even after renew() invalidated the token")
          .isZero();
    }

    @Test
    void unlockShouldWaitForAutoRenewFailureCallbackToComplete() throws Exception {
        CountDownLatch callbackStarted = new CountDownLatch(1);
        CountDownLatch callbackFinished = new CountDownLatch(1);
        AtomicBoolean callbackWasRunningDuringUnlock = new AtomicBoolean(false);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10))
          .withAutoRenew(Duration.ofMillis(100))
          .onAutoRenewFailure(ex -> {
              callbackStarted.countDown();
              try {
                  Thread.sleep(1000);
              } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
              }
              if (callbackFinished.getCount() > 0) {
                  callbackWasRunningDuringUnlock.set(true);
              }
              callbackFinished.countDown();
          })
          .build();

        RenewableLock lock = provider.forName("callback-join-test");
        lock.lock();

        expireLease("callback-join-test");

        assertThat(callbackStarted.await(5, TimeUnit.SECONDS))
          .as("callback should have started")
          .isTrue();

        assertThatThrownBy(lock::unlock)
          .isInstanceOf(LockNotHeldException.class);

        assertThat(callbackFinished.await(0, TimeUnit.SECONDS))
          .as("onAutoRenewFailure callback must have completed before unlock() returned")
          .isTrue();
    }

    @Test
    void unlockShouldNotBlockForeverWhenAutoRenewIsStuckInDb() throws Exception {
        CountDownLatch renewEnteredExecute = new CountDownLatch(1);
        CountDownLatch releaseHang = new CountDownLatch(1);

        DataSource hangingDs = hangingDataSource(dataSource, renewEnteredExecute, releaseHang);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(hangingDs, Duration.ofSeconds(10))
          .withAutoRenew(Duration.ofMillis(100))
          .build();

        RenewableLock lock = provider.forName("unlock-hang-test");
        lock.lock();

        try {
            assertThat(renewEnteredExecute.await(5, TimeUnit.SECONDS))
              .as("auto-renew thread should have entered executeUpdate before we try to unlock")
              .isTrue();

            CompletableFuture<Void> unlockFuture = CompletableFuture.runAsync(lock::unlock);

            assertThat(unlockFuture)
              .as("unlock() must not block indefinitely when auto-renew is stuck in a non-interruptible JDBC call")
              .succeedsWithin(Duration.ofSeconds(10));
        } finally {
            releaseHang.countDown();
        }
    }

    private static DataSource hangingDataSource(DataSource real, CountDownLatch entered, CountDownLatch release) {
        return (DataSource) Proxy.newProxyInstance(
          DataSource.class.getClassLoader(),
          new Class<?>[]{DataSource.class},
          (proxy, method, args) -> {
              if ("getConnection".equals(method.getName()) && (args == null || args.length == 0)) {
                  Connection raw = real.getConnection();
                  return hangingConnection(raw, entered, release);
              }
              return method.invoke(real, args);
          });
    }

    private static Connection hangingConnection(Connection real, CountDownLatch entered, CountDownLatch release) {
        return (Connection) Proxy.newProxyInstance(
          Connection.class.getClassLoader(),
          new Class<?>[]{Connection.class},
          (proxy, method, args) -> {
              if ("prepareStatement".equals(method.getName())
                && args != null && args.length > 0
                && args[0] instanceof String
                && ((String) args[0]).contains("SET expires_at = GREATEST")) {
                  PreparedStatement ps = real.prepareStatement((String) args[0]);
                  return hangingStatement(ps, entered, release);
              }
              return method.invoke(real, args);
          });
    }

    private static PreparedStatement hangingStatement(PreparedStatement real, CountDownLatch entered, CountDownLatch release) {
        return (PreparedStatement) Proxy.newProxyInstance(
          PreparedStatement.class.getClassLoader(),
          new Class<?>[]{PreparedStatement.class},
          (proxy, method, args) -> {
              if ("executeUpdate".equals(method.getName())) {
                  entered.countDown();
                  boolean waiting = true;
                  while (waiting) {
                      try {
                          release.await();
                          waiting = false;
                      } catch (InterruptedException ignored) {
                          // simulate pgjdbc socket read that ignores Thread.interrupt()
                      }
                  }
                  return 1;
              }
              return method.invoke(real, args);
          });
    }

    @Test
    void leaseLockWritesConfiguredInstanceId() throws SQLException {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(30))
            .withInstanceId("my-pod-7")
            .build();

        RenewableLock lock = provider.forName("instance-id-test");
        lock.lock();
        try {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement("SELECT locked_by FROM fencepost_locks WHERE lock_name = ?")) {
                ps.setString(1, "instance-id-test");
                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getString(1)).isEqualTo("my-pod-7");
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Test
    void lockWhileHeldShouldThrow() {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10)).build();

        RenewableLock lock = provider.forName("double-lock-test");
        lock.lock();

        try {
            assertThatThrownBy(lock::lock)
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("double-lock-test");
        } finally {
            lock.unlock();
        }
    }

    @Test
    void tryLockWhileHeldShouldThrow() {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10)).build();

        RenewableLock lock = provider.forName("double-trylock-test");
        lock.lock();

        try {
            assertThatThrownBy(lock::tryLock)
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("double-trylock-test");
        } finally {
            lock.unlock();
        }
    }

    @Test
    void advisoryLockShouldAcquireAndRelease() {
        LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(dataSource).build();

        AdvisoryLock lock = provider.forName("advisory-basic");
        lock.lock();
        lock.unlock();
    }

    @Test
    void advisoryTryLockShouldReturnFalseWhenHeld() {
        LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(dataSource).build();

        AdvisoryLock holder = provider.forName("advisory-contended");
        holder.lock();

        try {
            AdvisoryLock contender = provider.forName("advisory-contended");
            assertThat(contender.tryLock()).isFalse();
        } finally {
            holder.unlock();
        }
    }

    @Test
    void advisoryTryLockShouldReturnTrueWhenFree() {
        LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(dataSource).build();

        AdvisoryLock lock = provider.forName("advisory-free");
        assertThat(lock.tryLock()).isTrue();
        lock.unlock();
    }

    @Test
    void advisoryLockWithTimeoutShouldThrowOnTimeout() {
        LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(dataSource).build();

        AdvisoryLock holder = provider.forName("advisory-timeout");
        holder.lock();

        try {
            AdvisoryLock contender = provider.forName("advisory-timeout");
            assertThatThrownBy(() -> contender.lock(Duration.ofMillis(500)))
              .isInstanceOf(LockAcquisitionTimeoutException.class);
        } finally {
            holder.unlock();
        }
    }

    @Test
    void supplyLockedShouldReturnValue() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("supplylocked-return-test");

        long result = lock.supplyLocked(token -> token.value());

        assertThat(result).isGreaterThan(0);

        FencedLock second = provider.forName("supplylocked-return-test");
        Optional<FencingToken> secondToken = second.tryLock();
        assertThat(secondToken).isPresent();
        second.unlock();
    }

    @Test
    void supplyLockedWithTimeoutShouldReturnValue() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("supplylocked-return-timeout-test");

        long result = lock.supplyLocked(Duration.ofSeconds(5), token -> token.value());

        assertThat(result).isGreaterThan(0);

        FencedLock second = provider.forName("supplylocked-return-timeout-test");
        Optional<FencingToken> secondToken = second.tryLock();
        assertThat(secondToken).isPresent();
        second.unlock();
    }

    @Test
    void supplyLockedShouldPropagateUncheckedExceptionAndRelease() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("supplylocked-unchecked-test");

        assertThatThrownBy(() -> lock.supplyLocked(token -> {
            throw new IllegalArgumentException("boom");
        }))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("boom");

        FencedLock second = provider.forName("supplylocked-unchecked-test");
        Optional<FencingToken> secondToken = second.tryLock();
        assertThat(secondToken).isPresent();
        second.unlock();
    }

    @Test
    void supplyLockedShouldWrapCheckedExceptionAndRelease() {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock lock = provider.forName("supplylocked-checked-test");

        assertThatThrownBy(() -> lock.supplyLocked(token -> {
            throw new IOException("disk full");
        }))
          .isInstanceOf(FencepostException.class)
          .hasCauseInstanceOf(IOException.class);

        FencedLock second = provider.forName("supplylocked-checked-test");
        Optional<FencingToken> secondToken = second.tryLock();
        assertThat(secondToken).isPresent();
        second.unlock();
    }

    @Test
    void advisoryWithLockShouldAcquireRunAndRelease() {
        LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(dataSource).build();

        AdvisoryLock lock = provider.forName("advisory-withlock");
        AtomicBoolean ran = new AtomicBoolean(false);

        lock.runLocked(() -> ran.set(true));

        assertThat(ran.get()).isTrue();

        AdvisoryLock second = provider.forName("advisory-withlock");
        assertThat(second.tryLock()).isTrue();
        second.unlock();
    }

    @Test
    void advisorySupplyLockedShouldReturnValue() {
        LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(dataSource).build();

        AdvisoryLock lock = provider.forName("advisory-supplylocked-return");

        String result = lock.supplyLocked(() -> "hello");

        assertThat(result).isEqualTo("hello");

        AdvisoryLock second = provider.forName("advisory-supplylocked-return");
        assertThat(second.tryLock()).isTrue();
        second.unlock();
    }

    @Test
    void advisorySupplyLockedWithTimeoutShouldReturnValue() {
        LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(dataSource).build();

        AdvisoryLock lock = provider.forName("advisory-supplylocked-return-timeout");

        String result = lock.supplyLocked(Duration.ofSeconds(5), () -> "hello");

        assertThat(result).isEqualTo("hello");

        AdvisoryLock second = provider.forName("advisory-supplylocked-return-timeout");
        assertThat(second.tryLock()).isTrue();
        second.unlock();
    }

    @Test
    void advisoryTryWithResourcesShouldReleaseLock() {
        LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(dataSource).build();

        try (AdvisoryLock lock = provider.forName("advisory-auto-close")) {
            lock.lock();
        }

        AdvisoryLock second = provider.forName("advisory-auto-close");
        assertThat(second.tryLock()).isTrue();
        second.unlock();
    }

    @Test
    void advisoryLockShouldBlockUntilReleased() throws Exception {
        LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(dataSource).build();

        AdvisoryLock holder = provider.forName("advisory-blocking");
        holder.lock();

        CountDownLatch acquired = new CountDownLatch(1);

        Thread contender = new Thread(() -> {
            AdvisoryLock lock = provider.forName("advisory-blocking");
            lock.lock();
            acquired.countDown();
            lock.unlock();
        });
        contender.start();

        Thread.sleep(200);
        assertThat(acquired.getCount()).isEqualTo(1);

        holder.unlock();
        assertThat(acquired.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void advisoryConcurrentLocksShouldNotOverlap() throws Exception {
        LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(dataSource).build();

        AtomicBoolean overlap = new AtomicBoolean(false);
        AtomicBoolean inside = new AtomicBoolean(false);
        int iterations = 10;
        CountDownLatch done = new CountDownLatch(2);

        Runnable worker = () -> {
            for (int i = 0; i < iterations; i++) {
                AdvisoryLock lock = provider.forName("advisory-concurrent");
                lock.lock();
                if (inside.getAndSet(true)) {
                    overlap.set(true);
                }
                inside.set(false);
                lock.unlock();
            }
            done.countDown();
        };

        Thread t1 = new Thread(worker);
        Thread t2 = new Thread(worker);
        t1.start();
        t2.start();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(overlap.get()).isFalse();
    }

    @Test
    void sessionLockShouldBecomeAcquirableAfterConnectionDrop() throws Exception {
        LockFactory<FencedLock> provider = Fencepost.Locks.session(dataSource).build();

        FencedLock holder = provider.forName("conn-drop-session");
        FencingToken firstToken = holder.lock();

        terminateBackends("idle in transaction");

        FencedLock contender = provider.forName("conn-drop-session");
        Optional<FencingToken> secondToken = contender.tryLock();
        assertThat(secondToken).isPresent();
        assertThat(secondToken.get().value()).isGreaterThan(firstToken.value());
        contender.unlock();

        assertThatThrownBy(holder::unlock).isInstanceOf(FencepostException.class);
    }

    @Test
    void advisoryLockShouldBecomeAcquirableAfterConnectionDrop() throws Exception {
        LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(dataSource).build();

        AdvisoryLock holder = provider.forName("conn-drop-advisory");
        holder.lock();

        terminateBackends("idle");

        AdvisoryLock contender = provider.forName("conn-drop-advisory");
        assertThat(contender.tryLock()).isTrue();
        contender.unlock();

        assertThatThrownBy(holder::unlock).isInstanceOf(FencepostException.class);
    }

    @Test
    void advisoryUnlockShouldClearInheritedLocksOnPooledConnection() throws Exception {
        try (Connection sticky = dataSource.getConnection()) {
            long leakedKey = 4242424242L;
            try (PreparedStatement ps = sticky.prepareStatement("SELECT pg_advisory_lock(?)")) {
                ps.setLong(1, leakedKey);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
            }

            DataSource shared = sharedConnectionDataSource(sticky);
            LockFactory<AdvisoryLock> provider = Fencepost.Locks.advisory(shared).build();
            AdvisoryLock lock = provider.forName("unlock-all-inheritance");
            lock.lock();
            lock.unlock();

            try (Connection fresh = dataSource.getConnection();
                 PreparedStatement ps = fresh.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
                ps.setLong(1, leakedKey);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertThat(rs.getBoolean(1))
                      .as("leaked advisory lock on pooled connection should be cleared after unlock()")
                      .isTrue();
                }
            }
        }
    }

    private static DataSource sharedConnectionDataSource(Connection underlying) {
        Connection uncloseable = (Connection) Proxy.newProxyInstance(
          Connection.class.getClassLoader(),
          new Class<?>[]{Connection.class},
          (proxy, method, args) -> {
              if ("close".equals(method.getName())) {
                  return null;
              }
              return method.invoke(underlying, args);
          });
        return (DataSource) Proxy.newProxyInstance(
          DataSource.class.getClassLoader(),
          new Class<?>[]{DataSource.class},
          (proxy, method, args) -> {
              if ("getConnection".equals(method.getName())) {
                  return uncloseable;
              }
              throw new UnsupportedOperationException(method.getName());
          });
    }

    private static DataSource exhaustedPoolDataSource(DataSource real, int failAfter, int failCount) {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        return (DataSource) Proxy.newProxyInstance(
          DataSource.class.getClassLoader(),
          new Class<?>[]{DataSource.class},
          (proxy, method, args) -> {
              if ("getConnection".equals(method.getName()) && (args == null || args.length == 0)) {
                  int n = calls.incrementAndGet();
                  if (n > failAfter && failures.incrementAndGet() <= failCount) {
                      throw new SQLException("Connection pool exhausted (simulated)");
                  }
                  return real.getConnection();
              }
              return method.invoke(real, args);
          });
    }

    private void terminateBackends(String state) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement()
              .execute(String.format("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = '%s' AND pid != pg_backend_pid()", state));
        }
    }

    private long getExpiresAtEpoch(String lockName) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT extract(epoch from expires_at) FROM fencepost_locks WHERE lock_name = ?")) {
            ps.setString(1, lockName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private void expireLease(String lockName) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("UPDATE fencepost_locks SET expires_at = now() - interval '1 second' WHERE lock_name = ?")) {
            ps.setString(1, lockName);
            ps.executeUpdate();
        }
    }

    private boolean isExpired(String lockName) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT expires_at <= now() FROM fencepost_locks WHERE lock_name = ?")) {
            ps.setString(1, lockName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    @Test
    void leaseTryLockShouldNotBlockOnConcurrentRowLock() throws Exception {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10)).build();

        try (RenewableLock setup = provider.forName("skip-locked-test")) {
            setup.lock();
        }

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            conn.createStatement().executeQuery("SELECT 1 FROM fencepost_locks WHERE lock_name = 'skip-locked-test' FOR UPDATE");

            CompletableFuture<Optional<FencingToken>> future = CompletableFuture.supplyAsync(() -> {
                  try (RenewableLock contender = provider.forName("skip-locked-test")) {
                      return contender.tryLock();
                  }
              }, Executors.newSingleThreadExecutor());

            assertThat(future.get(2, TimeUnit.SECONDS))
              .as("tryLock should return empty immediately via SKIP LOCKED, not block on the row lock")
              .isEmpty();

            conn.rollback();
        }
    }

    @Test
    void shouldRejectLeaseAfterSessionForSameName() {
        LockFactory<FencedLock> sessionFactory = Fencepost.Locks.session(dataSource).build();
        LockFactory<RenewableLock> leaseFactory = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10)).build();

        FencedLock sessionLock = sessionFactory.forName("mixed-type-test");
        sessionLock.lock();
        sessionLock.unlock();

        RenewableLock leaseLock = leaseFactory.forName("mixed-type-test");
        assertThatThrownBy(leaseLock::lock)
          .isInstanceOf(FencepostException.class)
          .hasMessageContaining("mixed-type-test");
    }

    @Test
    void shouldRejectSessionAfterLeaseForSameName() {
        LockFactory<RenewableLock> leaseFactory = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10)).build();
        LockFactory<FencedLock> sessionFactory = Fencepost.Locks.session(dataSource).build();

        RenewableLock leaseLock = leaseFactory.forName("mixed-type-test-2");
        leaseLock.lock();
        leaseLock.unlock();

        FencedLock sessionLock = sessionFactory.forName("mixed-type-test-2");
        assertThatThrownBy(sessionLock::lock)
          .isInstanceOf(FencepostException.class)
          .hasMessageContaining("mixed-type-test-2");
    }

    @Test
    void shouldAllowSameTypeReuseForSession() {
        LockFactory<FencedLock> factory1 = Fencepost.Locks.session(dataSource).build();
        LockFactory<FencedLock> factory2 = Fencepost.Locks.session(dataSource).build();

        FencedLock lock1 = factory1.forName("same-type-session");
        lock1.lock();
        lock1.unlock();

        FencedLock lock2 = factory2.forName("same-type-session");
        FencingToken token = lock2.lock();
        assertThat(token).isNotNull();
        lock2.unlock();
    }

    @Test
    void shouldRetryTokenAllocationOnPoolExhaustion() {
        DataSource limited = exhaustedPoolDataSource(dataSource, 1, 1);
        LockFactory<FencedLock> provider = Fencepost.Locks.session(limited).build();

        FencedLock lock = provider.forName("retry-success-test");
        FencingToken token = lock.lock();
        assertThat(token).isNotNull();
        lock.unlock();
    }

    @Test
    void shouldFailWithClearErrorWhenTokenAllocationRetriesExhausted() {
        DataSource limited = exhaustedPoolDataSource(dataSource, 1, 3);
        LockFactory<FencedLock> provider = Fencepost.Locks.session(limited).build();

        FencedLock lock = provider.forName("retry-exhausted-test");
        assertThatThrownBy(lock::lock)
          .isInstanceOf(FencepostException.class)
          .hasMessageContaining("Failed to acquire lock")
          .cause()
          .hasMessageContaining("connection pool may be exhausted");
    }

    @Test
    void lockTimeoutShouldNotOvershootByPollInterval() {
        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(30))
          .withPollInterval(Duration.ofSeconds(5))
          .build();

        RenewableLock holder = provider.forName("timeout-overshoot-test");
        holder.lock();

        try {
            RenewableLock contender = provider.forName("timeout-overshoot-test");
            long start = System.nanoTime();
            assertThatThrownBy(() -> contender.lock(Duration.ofMillis(500)))
              .isInstanceOf(LockAcquisitionTimeoutException.class);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertThat(elapsedMs)
              .as("lock() should time out near the requested 500ms, not after a full 5s poll interval")
              .isLessThan(2000);
        } finally {
            holder.unlock();
        }
    }

    @Test
    void timedSessionLockShouldCapTokenAllocationToDeadline() {
        DataSource limited = exhaustedPoolDataSource(dataSource, 1, 10);
        LockFactory<FencedLock> provider = Fencepost.Locks.session(limited).build();

        FencedLock lock = provider.forName("timed-token-alloc-test");
        long start = System.nanoTime();
        assertThatThrownBy(() -> lock.lock(Duration.ofMillis(500)))
          .isInstanceOf(FencepostException.class);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertThat(elapsedMs)
          .as("lock(Duration) should not overshoot far past deadline during token allocation retries")
          .isLessThan(2000);
    }

    @Test
    void tokenAllocationShouldRestoreNetworkTimeoutOnPooledConnection() {
        List<Integer> networkTimeoutsAtClose = new CopyOnWriteArrayList<>();

        DataSource tracking = networkTimeoutTrackingDataSource(dataSource, 1, networkTimeoutsAtClose);
        LockFactory<FencedLock> provider = Fencepost.Locks.session(tracking).build();

        FencedLock lock = provider.forName("restore-timeout-test");
        lock.lock();
        lock.unlock();

        assertThat(networkTimeoutsAtClose).isNotEmpty();
        assertThat(networkTimeoutsAtClose).allSatisfy(timeout ->
          assertThat(timeout).as("networkTimeout should be restored to original value before close").isZero()
        );
    }

    private static DataSource networkTimeoutTrackingDataSource(DataSource real, int skipCalls, List<Integer> closingTimeouts) {
        AtomicInteger calls = new AtomicInteger();
        return (DataSource) Proxy.newProxyInstance(
          DataSource.class.getClassLoader(),
          new Class<?>[]{DataSource.class},
          (proxy, method, args) -> {
              if ("getConnection".equals(method.getName()) && (args == null || args.length == 0)) {
                  Connection conn = real.getConnection();
                  int n = calls.incrementAndGet();
                  if (n > skipCalls) {
                      return trackingTimeoutConnection(conn, closingTimeouts);
                  }
                  return conn;
              }
              return method.invoke(real, args);
          });
    }

    private static Connection trackingTimeoutConnection(Connection real, List<Integer> closingTimeouts) {
        return (Connection) Proxy.newProxyInstance(
          Connection.class.getClassLoader(),
          new Class<?>[]{Connection.class},
          (proxy, method, args) -> {
              if ("close".equals(method.getName())) {
                  closingTimeouts.add(real.getNetworkTimeout());
                  return method.invoke(real, args);
              }
              return method.invoke(real, args);
          });
    }

    @Test
    void shouldAllowSameTypeReuseForLease() {
        LockFactory<RenewableLock> factory1 = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(10)).build();
        LockFactory<RenewableLock> factory2 = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(5)).build();

        RenewableLock lock1 = factory1.forName("same-type-lease");
        lock1.lock();
        lock1.unlock();

        RenewableLock lock2 = factory2.forName("same-type-lease");
        FencingToken token = lock2.lock();
        assertThat(token).isNotNull();
        lock2.unlock();
    }
}
