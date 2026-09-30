package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.LockAcquisitionTimeoutException;
import com.pivovarit.fencepost.lock.LockNotHeldException;
import com.pivovarit.fencepost.lock.RenewableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Not thread-safe. Each instance should be used by a single thread at a time.
 * For concurrent access, create separate instances via {@code Factory.forName}.
 */
final class LeaseLockInstance extends TableBasedLock implements RenewableLock {

    private static final Logger logger = LoggerFactory.getLogger(LeaseLockInstance.class);
    private static final long DEFAULT_POLL_INTERVAL_MS = 100;
    private static final int AUTO_RENEW_MAX_RETRIES = 3;
    private static final long STOP_AUTO_RENEW_JOIN_TIMEOUT_MS = 5_000;

    private final Duration leaseDuration;
    private final long leaseDurationMs;
    private final Duration refreshInterval;
    private final Duration autoRenewStatementTimeout;
    private final long autoRenewCycleBudgetMillis;
    private final Duration quietPeriod;
    private final long quietPeriodMs;
    private final long pollIntervalMs;
    private final Consumer<FencepostException> onAutoRenewFailure;
    private final String instanceId;
    private final Sql sql;
    private final AtomicReference<Thread> owner = new AtomicReference<>();

    private volatile Thread autoRenewThread;
    private volatile long autoRenewWindowMillis;
    private volatile PreparedStatement autoRenewStatement;
    private volatile boolean autoRenewStopped;

    LeaseLockInstance(String lockName, DataSource dataSource, String tableName,
                         Duration leaseDuration, Duration refreshInterval, Duration quietPeriod,
                         Duration pollInterval,
                         Consumer<FencepostException> onAutoRenewFailure,
                         String instanceId) {
        super(lockName, dataSource, tableName, LockType.LEASE);
        this.leaseDuration = leaseDuration;
        this.leaseDurationMs = leaseDuration.toMillis();
        this.refreshInterval = refreshInterval;
        if (refreshInterval != null) {
            validateAutoRenewDetectionBudget(leaseDurationMs, refreshInterval.toMillis());
            this.autoRenewStatementTimeout = Duration.ofMillis(
                autoRenewAttemptTimeoutMillis(leaseDurationMs, refreshInterval.toMillis()));
            this.autoRenewCycleBudgetMillis = autoRenewCycleBudgetMillis(leaseDurationMs, refreshInterval.toMillis());
        } else {
            this.autoRenewStatementTimeout = null;
            this.autoRenewCycleBudgetMillis = 0;
        }
        this.quietPeriod = quietPeriod;
        this.quietPeriodMs = quietPeriod != null ? quietPeriod.toMillis() : 0;
        this.pollIntervalMs = pollInterval != null ? Durations.toPositiveMillis(pollInterval, "pollInterval") : DEFAULT_POLL_INTERVAL_MS;
        this.onAutoRenewFailure = onAutoRenewFailure;
        this.instanceId = instanceId;
        this.sql = new Sql(tableName);
    }

    static final class Sql {
        final String acquire;
        final String renew;
        final String unlockWithQuietPeriod;
        final String unlock;

        Sql(String tableName) {
            this.acquire = """
                UPDATE %s SET token = token + 1, locked_by = ?, locked_at = clock_timestamp(), expires_at = clock_timestamp() + %s
                WHERE lock_name IN (
                  SELECT lock_name FROM %s
                  WHERE lock_name = ? AND (locked_by IS NULL OR expires_at IS NULL OR expires_at <= clock_timestamp())
                  FOR UPDATE SKIP LOCKED)
                RETURNING token""".formatted(tableName, Jdbc.intervalMillis(), tableName);
            this.renew = """
                UPDATE %s SET expires_at = GREATEST(expires_at, now() + %s)
                %s""".formatted(tableName, Jdbc.intervalMillis(), activeLeasePredicate());
            this.unlockWithQuietPeriod = """
                UPDATE %s SET token = token + 1, locked_at = NULL, expires_at = now() + %s
                %s""".formatted(tableName, Jdbc.intervalMillis(), activeLeasePredicate());
            this.unlock = """
                UPDATE %s SET token = token + 1, locked_by = NULL, locked_at = NULL, expires_at = NULL
                %s""".formatted(tableName, activeLeasePredicate());
        }
    }

    @Override
    public FencingToken lock() {
        acquireOwnership();
        try {
            return doLock();
        } catch (Exception e) {
            releaseOwnership();
            throw e;
        }
    }

    @Override
    public FencingToken lock(Duration timeout) {
        Durations.requireAtLeastOneMillisecond(timeout, "timeout");
        acquireOwnership();
        try {
            return doLock(timeout);
        } catch (Exception e) {
            releaseOwnership();
            throw e;
        }
    }

    @Override
    public Optional<FencingToken> tryLock() {
        acquireOwnership();
        try {
            Optional<FencingToken> result = doTryLock();
            if (result.isEmpty()) {
                releaseOwnership();
            }
            return result;
        } catch (Exception e) {
            releaseOwnership();
            throw e;
        }
    }

    @Override
    FencingToken doLock() {
        return lockTimestampBlocking(null);
    }

    @Override
    FencingToken doLock(Duration timeout) {
        return lockTimestampBlocking(timeout);
    }

    @Override
    Optional<FencingToken> doTryLock() {
        ensureRowExists();
        Optional<FencingToken> result = tryAcquireTimestamp();
        if (result.isPresent()) {
            currentToken = result.get();
            logger.debug("acquired lease lock '{}' via tryLock, token={}, lease={}", lockName, currentToken.value(), leaseDuration);
            if (hasAutoRenew()) {
                startAutoRenew();
            }
        } else {
            logger.debug("tryLock failed for lease lock '{}' - already held", lockName);
        }
        return result;
    }

    private FencingToken lockTimestampBlocking(Duration timeout) {
        long deadlineNanos = timeout != null
            ? System.nanoTime() + timeout.toNanos()
            : Long.MAX_VALUE;

        ensureRowExists();

        while (true) {
            if (timeout != null && System.nanoTime() >= deadlineNanos) {
                logger.debug("timed out acquiring lease lock '{}' after {}", lockName, timeout);
                throw new LockAcquisitionTimeoutException(lockName);
            }

            Optional<FencingToken> result = tryAcquireTimestamp();
            if (result.isPresent()) {
                currentToken = result.get();
                logger.debug("acquired lease lock '{}', token={}, lease={}", lockName, currentToken.value(), leaseDuration);
                if (hasAutoRenew()) {
                    startAutoRenew();
                }
                return currentToken;
            }

            try {
                long sleepMs = pollIntervalMs;
                if (timeout != null) {
                    long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
                    if (remainingMs <= 0) {
                        logger.debug("timed out acquiring lease lock '{}' after {}", lockName, timeout);
                        throw new LockAcquisitionTimeoutException(lockName);
                    }
                    sleepMs = Math.min(sleepMs, remainingMs);
                }
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FencepostException("Interrupted while waiting for lock: " + lockName);
            }
        }
    }

    private Optional<FencingToken> tryAcquireTimestamp() {
        String lockedBy = instanceId != null
            ? instanceId
            : String.format("%s/%s", HOSTNAME, Thread.currentThread().getName());

        try {
            return Jdbc.query(dataSource, sql.acquire)
                    .bind(lockedBy)
                    .bind(leaseDurationMs)
                    .bind(lockName)
                    .map(rs -> rs.next() ? Optional.of(new FencingToken(rs.getLong(1))) : Optional.empty());
        } catch (SQLException e) {
            throw new FencepostException("Failed to acquire lock: " + lockName, e);
        }
    }

    @Override
    public boolean isSuperseded(FencingToken token) {
        return checkSuperseded(token);
    }

    @Override
    public void renew(Duration duration) {
        long durationMillis = Durations.toPositiveMillis(duration, "duration");
        FencingToken token = currentToken;
        if (token == null) {
            throw new LockNotHeldException(lockName);
        }
        try {
            int updated = Jdbc.update(dataSource, sql.renew)
                    .bind(durationMillis)
                    .bind(lockName)
                    .bind(token.value())
                    .execute();
            if (updated == 0) {
                releaseLostLease(token.value());
                throw new LockNotHeldException(lockName);
            }
            logger.debug("renewed lease lock '{}', token={}, duration={}", lockName, token.value(), duration);
            if (autoRenewThread != null) {
                autoRenewWindowMillis = durationMillis;
            }
        } catch (LockNotHeldException e) {
            throw e;
        } catch (SQLException e) {
            throw new FencepostException("Failed to renew lock: " + lockName, e);
        }
    }

    @Override
    public void unlock() {
        stopAutoRenew();
        FencingToken token = currentToken;
        if (token == null) {
            throw new LockNotHeldException(lockName);
        }
        try {
            int updated;
            if (quietPeriod != null) {
                updated = Jdbc.update(dataSource, sql.unlockWithQuietPeriod)
                        .bind(quietPeriodMs)
                        .bind(lockName)
                        .bind(token.value())
                        .execute();
            } else {
                updated = Jdbc.update(dataSource, sql.unlock)
                        .bind(lockName)
                        .bind(token.value())
                        .execute();
            }
            if (updated == 0) {
                throw new LockNotHeldException(lockName);
            }
            logger.debug("released lease lock '{}', token={}", lockName, token.value());
        } catch (SQLException e) {
            throw new FencepostException("Failed to release lock: " + lockName, e);
        } finally {
            currentToken = null;
            releaseOwnership();
        }
    }

    @Override
    public void close() {
        stopAutoRenew();
        if (currentToken != null) {
            try {
                unlock();
            } catch (Exception e) {
                logger.trace("failed to unlock lease lock '{}' during close", lockName, e);
            }
        }
    }

    private boolean hasAutoRenew() {
        return refreshInterval != null;
    }

    private void startAutoRenew() {
        long token = currentToken.value();
        autoRenewStopped = false;
        autoRenewWindowMillis = leaseDurationMs;
        logger.debug("starting auto-renew for lease lock '{}', token={}, interval={}", lockName, token, refreshInterval);
        autoRenewThread = new Thread(() -> {
            long intervalMillis = refreshInterval.toMillis();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(intervalMillis);
                    refreshWithRetry(autoRenewWindowMillis, token);
                    logger.trace("auto-renewed lease lock '{}', token={}", lockName, token);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (SQLException | RuntimeException | Error e) {
                    if (!autoRenewStopped) {
                        logger.warn("auto-renew failed for lease lock '{}', token={}", lockName, token, e);
                        FencepostException ex = new FencepostException("Auto-renew failed for lock: " + lockName, e);
                        if (onAutoRenewFailure != null) {
                            onAutoRenewFailure.accept(ex);
                        }
                    }
                    if (e instanceof Error error) {
                        throw error;
                    }
                    return;
                }
            }
        });
        autoRenewThread.setDaemon(true);
        autoRenewThread.setName("fencepost-auto-renew-" + lockName);
        autoRenewThread.start();
    }

    /**
     * Retries the renew under an absolute deadline of
     * {@link #autoRenewCycleBudgetMillis} per cycle, so that time spent anywhere
     * in an attempt — including pool checkout, which no statement or network
     * timeout covers — is charged against the loss-detection budget. Once the
     * deadline cannot fund another backoff and attempt, the loss is reported
     * immediately instead of retrying into the standby's acquisition window.
     *
     * <p>Unchecked exceptions are retried on the same terms as {@link SQLException}:
     * a pool or routing {@link DataSource} that throws a {@link RuntimeException}
     * from {@code getConnection()} is just as transient as a refused connection,
     * and must not skip the retry budget and report loss while the lease is valid.
     */
    private void refreshWithRetry(long windowMillis, long token) throws SQLException, InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(autoRenewCycleBudgetMillis);
        Exception lastException = null;
        for (int attempt = 0; attempt < AUTO_RENEW_MAX_RETRIES; attempt++) {
            try {
                int updated = renewWithStatementTimeout(windowMillis, token, deadlineNanos);
                if (updated == 0) {
                    throw new SQLException("Lock lost - token no longer matches or lease expired");
                }
                return;
            } catch (SQLException | RuntimeException e) {
                lastException = e;
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException();
                }
                if (attempt < AUTO_RENEW_MAX_RETRIES - 1) {
                    long backoffMillis = 100L * (attempt + 1);
                    if (TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()) <= backoffMillis) {
                        throw renewFailure(lastException);
                    }
                    Thread.sleep(backoffMillis);
                }
            } finally {
                autoRenewStatement = null;
            }
        }
        throw renewFailure(lastException);
    }

    private static SQLException renewFailure(Exception e) throws SQLException {
        if (e instanceof RuntimeException runtime) {
            throw runtime;
        }
        throw (SQLException) e;
    }

    /**
     * Renews the lease under a millisecond-precise, server-enforced
     * {@code statement_timeout}. JDBC {@link java.sql.Statement#setQueryTimeout}
     * only supports whole-second granularity, which floored sub-second renew
     * windows up to a full second and let worst-case loss detection exceed the
     * lease duration. {@code SET LOCAL statement_timeout} requires a transaction,
     * so autocommit is disabled for the renew and restored afterwards.
     *
     * <p>{@code statement_timeout} only cancels server-side execution; if the TCP
     * connection is blackholed (no RST), the client would block at the OS retransmit
     * timeout instead. The same budget is therefore also applied as the
     * connection's network timeout so every round trip on the renew connection is
     * client-side bounded too.
     *
     * <p>Establishing the connection itself is bounded only by the driver/pool
     * ({@code connectTimeout}, checkout timeout), so the time it takes is measured
     * and charged against the cycle deadline instead: the statement gets
     * {@code min(per-attempt budget, time left until the deadline)}, and a checkout
     * that returns with no budget left fails the attempt without touching the
     * database. The one case this cannot bound is a checkout still blocked when the
     * deadline passes — detection then happens as soon as it returns, late by at
     * most the pool's checkout timeout. Keeping that timeout at or below the
     * per-attempt budget ({@link #autoRenewAttemptTimeoutMillis}) restores the
     * strict bound; a breach only delays detection, never unfences writes.
     */
    private int renewWithStatementTimeout(long windowMillis, long token, long deadlineNanos) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            long budgetMillis = Math.min(
                autoRenewStatementTimeout.toMillis(),
                TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
            if (budgetMillis < 1) {
                throw new SQLException("Renew connection checkout consumed the remaining loss-detection budget for lock: " + lockName);
            }
            Duration attemptTimeout = Duration.ofMillis(budgetMillis);
            int previousNetworkTimeout = connection.getNetworkTimeout();
            connection.setNetworkTimeout(Runnable::run, (int) Math.min(budgetMillis, Integer.MAX_VALUE));
            boolean previousAutoCommit = connection.getAutoCommit();
            if (previousAutoCommit) {
                connection.setAutoCommit(false);
            }
            try {
                Jdbc.setStatementTimeout(connection, attemptTimeout);
                int updated = Jdbc.update(connection, sql.renew)
                        .onStatement(ps -> autoRenewStatement = ps)
                        .bind(windowMillis)
                        .bind(lockName)
                        .bind(token)
                        .execute();
                connection.commit();
                return updated;
            } catch (SQLException e) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    e.addSuppressed(rollbackFailure);
                }
                throw e;
            } finally {
                if (previousAutoCommit) {
                    try {
                        connection.setAutoCommit(true);
                    } catch (SQLException e) {
                        logger.trace("failed to restore autoCommit on renew connection for lease lock '{}'", lockName, e);
                    }
                }
                try {
                    connection.setNetworkTimeout(Runnable::run, previousNetworkTimeout);
                } catch (SQLException e) {
                    logger.trace("failed to restore network timeout on renew connection for lease lock '{}'", lockName, e);
                }
            }
        }
    }

    private static long autoRenewSafetyMarginMillis(long leaseMillis) {
        return Math.max(1, leaseMillis / 10);
    }

    private static long autoRenewTotalBackoffMillis() {
        long total = 0;
        for (int attempt = 0; attempt < AUTO_RENEW_MAX_RETRIES - 1; attempt++) {
            total += 100L * (attempt + 1);
        }
        return total;
    }

    private static long autoRenewAttemptBudgetMillis(long leaseMillis, long refreshMillis) {
        return leaseMillis - refreshMillis - autoRenewTotalBackoffMillis() - autoRenewSafetyMarginMillis(leaseMillis);
    }

    /**
     * Per-attempt {@code statement_timeout} sized so that all renew retries plus
     * their backoff complete within the lease, with a safety margin, regardless
     * of how slow the database is. Never larger than the refresh interval.
     */
    static long autoRenewAttemptTimeoutMillis(long leaseMillis, long refreshMillis) {
        long perAttempt = Math.max(1, autoRenewAttemptBudgetMillis(leaseMillis, refreshMillis) / AUTO_RENEW_MAX_RETRIES);
        return Math.min(perAttempt, refreshMillis);
    }

    /**
     * Absolute time budget for one renew cycle: {@value #AUTO_RENEW_MAX_RETRIES}
     * attempts each bounded by {@link #autoRenewAttemptTimeoutMillis}, plus
     * inter-attempt backoff. Enforced as a deadline by the renew loop, so it
     * covers everything an attempt does — including pool checkout, which no
     * per-statement timeout can bound.
     */
    static long autoRenewCycleBudgetMillis(long leaseMillis, long refreshMillis) {
        return (long) AUTO_RENEW_MAX_RETRIES * autoRenewAttemptTimeoutMillis(leaseMillis, refreshMillis)
            + autoRenewTotalBackoffMillis();
    }

    /**
     * Worst-case time from the last successful renew to reporting the loss: the
     * refresh interval, then one full cycle budget. Must stay strictly below the
     * lease so a standby cannot acquire while the old leader still believes it
     * holds the lease.
     */
    static long worstCaseAutoRenewDetectionMillis(long leaseMillis, long refreshMillis) {
        return refreshMillis + autoRenewCycleBudgetMillis(leaseMillis, refreshMillis);
    }

    static void validateAutoRenewDetectionBudget(long leaseMillis, long refreshMillis) {
        if (autoRenewAttemptBudgetMillis(leaseMillis, refreshMillis) < AUTO_RENEW_MAX_RETRIES) {
            throw new IllegalArgumentException(
                "auto-renew cannot detect lease loss before expiry: leaseDuration=" + leaseMillis
                    + "ms with refreshInterval=" + refreshMillis + "ms leaves no time budget to retry renewals before the"
                    + " lease expires (needs room for " + AUTO_RENEW_MAX_RETRIES + " attempts, "
                    + autoRenewTotalBackoffMillis() + "ms backoff and a " + autoRenewSafetyMarginMillis(leaseMillis)
                    + "ms safety margin within the lease). Increase leaseDuration or decrease refreshInterval.");
        }
    }

    private static String activeLeasePredicate() {
        return "WHERE lock_name = ? AND token = ? AND expires_at > now()";
    }

    private void acquireOwnership() {
        Thread current = Thread.currentThread();
        Thread existing = owner.compareAndExchange(null, current);
        if (existing != null) {
            if (existing == current) {
                throw new IllegalStateException("Lock already held by this thread: " + lockName);
            }
            throw new IllegalStateException(
                "LeaseLockInstance is not thread-safe - already in use by thread '"
                    + existing.getName() + "', called from thread '" + current.getName()
                    + "'. Create separate instances via Factory.forName for concurrent access.");
        }
    }

    private void releaseOwnership() {
        owner.set(null);
    }

    private void releaseLostLease(long token) {
        FencingToken held = currentToken;
        if (held != null && held.value() == token) {
            stopAutoRenew();
            currentToken = null;
            releaseOwnership();
        }
    }

    private void stopAutoRenew() {
        Thread thread = autoRenewThread;
        if (thread == null) {
            return;
        }
        autoRenewStopped = true;
        thread.interrupt();
        PreparedStatement stmt = autoRenewStatement;
        if (stmt != null) {
            try {
                stmt.cancel();
            } catch (SQLException e) {
                logger.trace("cancel of in-flight auto-renew statement failed for lease lock '{}'", lockName, e);
            }
        }
        try {
            thread.join(STOP_AUTO_RENEW_JOIN_TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (thread.isAlive()) {
            logger.warn("auto-renew thread for lease lock '{}' did not exit within {} ms; detaching (daemon)", lockName, STOP_AUTO_RENEW_JOIN_TIMEOUT_MS);
        }
        autoRenewThread = null;
        autoRenewStatement = null;
    }
}
