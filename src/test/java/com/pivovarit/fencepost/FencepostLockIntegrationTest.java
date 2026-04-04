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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
              .execute("DROP TABLE IF EXISTS fencepost_locks; CREATE TABLE fencepost_locks (  lock_name TEXT PRIMARY KEY,  token BIGINT NOT NULL DEFAULT 0,  locked_by TEXT,  locked_at TIMESTAMP WITH TIME ZONE,  expires_at TIMESTAMP WITH TIME ZONE)");
        }
    }

    @Test
    void shouldAcquireAndReleaseLock() {
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

        FencedLock lock = provider.forName("test-lock");
        FencingToken token = lock.lock();

        assertThat(token).isNotNull();
        assertThat(token.value()).isGreaterThan(0);

        lock.unlock();
    }

    @Test
    void shouldReturnStrictlyIncreasingTokens() {
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

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
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

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
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

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
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

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
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

        FencedLock lock = provider.forName("not-held");
        assertThatThrownBy(lock::unlock)
          .isInstanceOf(LockNotHeldException.class);
    }

    @Test
    void tryWithResourcesShouldReleaseLock() {
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

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
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

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
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

        FencedLock lock = provider.forName("withlock-test");
        AtomicReference<FencingToken> capturedToken = new AtomicReference<>();

        lock.withLock(capturedToken::set);

        assertThat(capturedToken.get()).isNotNull();
        assertThat(capturedToken.get().value()).isGreaterThan(0);

        FencedLock second = provider.forName("withlock-test");
        Optional<FencingToken> secondToken = second.tryLock();
        assertThat(secondToken).isPresent();
        second.unlock();
    }

    @Test
    void withLockWithTimeoutShouldAcquireRunAndRelease() {
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

        FencedLock lock = provider.forName("withlock-timeout-test");
        AtomicReference<FencingToken> capturedToken = new AtomicReference<>();

        lock.withLock(Duration.ofSeconds(5), capturedToken::set);

        assertThat(capturedToken.get()).isNotNull();
        assertThat(capturedToken.get().value()).isGreaterThan(0);

        FencedLock second = provider.forName("withlock-timeout-test");
        Optional<FencingToken> secondToken = second.tryLock();
        assertThat(secondToken).isPresent();
        second.unlock();
    }

    @Test
    void withLockShouldPropagateUncheckedExceptionAndRelease() {
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

        FencedLock lock = provider.forName("withlock-unchecked-test");

        assertThatThrownBy(() -> lock.withLock(token -> {
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
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

        FencedLock lock = provider.forName("withlock-checked-test");

        assertThatThrownBy(() -> lock.withLock(token -> {
            throw new java.io.IOException("disk full");
        }))
          .isInstanceOf(FencepostException.class)
          .hasCauseInstanceOf(java.io.IOException.class);

        FencedLock second = provider.forName("withlock-checked-test");
        Optional<FencingToken> secondToken = second.tryLock();
        assertThat(secondToken).isPresent();
        second.unlock();
    }

    @Test
    void withLockWhileHeldShouldThrow() {
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

        FencedLock lock = provider.forName("withlock-guard-test");
        lock.lock();

        try {
            assertThatThrownBy(() -> lock.withLock(token -> {}))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("withlock-guard-test");
        } finally {
            lock.unlock();
        }
    }

    @Test
    void ttlExpiredLockShouldBeAcquirable() throws Exception {
        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(1)).build();

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
    void heartbeatShouldExtendExpiry() throws Exception {
        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(2))
            .withHeartbeat(Duration.ofSeconds(1))
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
    void renewShouldExtendExpiry() throws Exception {
        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(2)).build();

        RenewableLock lock = provider.forName("renew-test");
        lock.lock();

        long initialExpiry = getExpiresAtEpoch("renew-test");

        lock.renew(Duration.ofSeconds(30));

        long renewedExpiry = getExpiresAtEpoch("renew-test");
        assertThat(renewedExpiry).isGreaterThan(initialExpiry);

        lock.unlock();
    }

    @Test
    void renewShouldUpdateHeartbeatWindow() throws Exception {
        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(2))
            .withHeartbeat(Duration.ofSeconds(1))
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
        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(2)).build();

        RenewableLock lock = provider.forName("renew-not-held");
        assertThatThrownBy(() -> lock.renew(Duration.ofSeconds(5)))
          .isInstanceOf(LockNotHeldException.class);
    }

    @Test
    void renewWithZeroDurationShouldThrow() {
        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(2)).build();

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
        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(2)).build();

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
        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(10)).build();

        RenewableLock lock = provider.forName("superseded-test");
        FencingToken token = lock.lock();

        assertThat(lock.isSuperseded(token)).isFalse();

        lock.unlock();
    }

    @Test
    void isSupersededShouldReturnTrueForOldToken() {
        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(10)).build();

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
        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(10)).build();

        RenewableLock lock = provider.forName("nonexistent-lock");

        assertThatThrownBy(() -> lock.isSuperseded(new FencingToken(1)))
          .isInstanceOf(FencepostException.class)
          .hasMessageContaining("not found");
    }

    @Test
    void quietPeriodShouldPreventImmediateReacquisition() throws Exception {
        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(10))
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
    void onHeartbeatFailureShouldBeCalledWhenLockIsStolen() throws Exception {
        AtomicBoolean callbackFired = new AtomicBoolean(false);
        AtomicReference<FencepostException> callbackError = new AtomicReference<>();

        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(2))
            .withHeartbeat(Duration.ofSeconds(1))
            .onHeartbeatFailure(ex -> {
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
    void heartbeatFailureShouldInvalidateCurrentToken() throws Exception {
        AtomicBoolean callbackFired = new AtomicBoolean(false);

        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(2))
            .withHeartbeat(Duration.ofSeconds(1))
            .onHeartbeatFailure(ex -> callbackFired.set(true))
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
    void lockWhileHeldShouldThrow() {
        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(10)).build();

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
        Factory<RenewableLock> provider = Fencepost.leaseLock(dataSource, Duration.ofSeconds(10)).build();

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
        Factory<AdvisoryLock> provider = Fencepost.advisoryLock(dataSource).build();

        AdvisoryLock lock = provider.forName("advisory-basic");
        lock.lock();
        lock.unlock();
    }

    @Test
    void advisoryTryLockShouldReturnFalseWhenHeld() {
        Factory<AdvisoryLock> provider = Fencepost.advisoryLock(dataSource).build();

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
        Factory<AdvisoryLock> provider = Fencepost.advisoryLock(dataSource).build();

        AdvisoryLock lock = provider.forName("advisory-free");
        assertThat(lock.tryLock()).isTrue();
        lock.unlock();
    }

    @Test
    void advisoryLockWithTimeoutShouldThrowOnTimeout() {
        Factory<AdvisoryLock> provider = Fencepost.advisoryLock(dataSource).build();

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
    void advisoryWithLockShouldAcquireRunAndRelease() {
        Factory<AdvisoryLock> provider = Fencepost.advisoryLock(dataSource).build();

        AdvisoryLock lock = provider.forName("advisory-withlock");
        AtomicBoolean ran = new AtomicBoolean(false);

        lock.withLock(() -> ran.set(true));

        assertThat(ran.get()).isTrue();

        AdvisoryLock second = provider.forName("advisory-withlock");
        assertThat(second.tryLock()).isTrue();
        second.unlock();
    }

    @Test
    void advisoryTryWithResourcesShouldReleaseLock() {
        Factory<AdvisoryLock> provider = Fencepost.advisoryLock(dataSource).build();

        try (AdvisoryLock lock = provider.forName("advisory-auto-close")) {
            lock.lock();
        }

        AdvisoryLock second = provider.forName("advisory-auto-close");
        assertThat(second.tryLock()).isTrue();
        second.unlock();
    }

    @Test
    void advisoryLockShouldBlockUntilReleased() throws Exception {
        Factory<AdvisoryLock> provider = Fencepost.advisoryLock(dataSource).build();

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
        Factory<AdvisoryLock> provider = Fencepost.advisoryLock(dataSource).build();

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
        Factory<FencedLock> provider = Fencepost.sessionLock(dataSource).build();

        FencedLock holder = provider.forName("conn-drop-session");
        holder.lock();

        terminateBackends("idle in transaction");

        FencedLock contender = provider.forName("conn-drop-session");
        assertThat(contender.tryLock()).isPresent();
        contender.unlock();

        assertThatThrownBy(holder::unlock).isInstanceOf(FencepostException.class);
    }

    @Test
    void advisoryLockShouldBecomeAcquirableAfterConnectionDrop() throws Exception {
        Factory<AdvisoryLock> provider = Fencepost.advisoryLock(dataSource).build();

        AdvisoryLock holder = provider.forName("conn-drop-advisory");
        holder.lock();

        terminateBackends("idle");

        AdvisoryLock contender = provider.forName("conn-drop-advisory");
        assertThat(contender.tryLock()).isTrue();
        contender.unlock();

        assertThatThrownBy(holder::unlock).isInstanceOf(FencepostException.class);
    }

    private void terminateBackends(String state) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(String.format("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = '%s' AND pid != pg_backend_pid()", state));
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
}
