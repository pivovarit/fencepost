package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.LockAcquisitionTimeoutException;
import com.pivovarit.fencepost.lock.LockNotHeldException;
import com.pivovarit.fencepost.lock.RenewableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
    private final Duration quietPeriod;
    private final long quietPeriodMs;
    private final long pollIntervalMs;
    private final Consumer<FencepostException> onAutoRenewFailure;
    private final String instanceId;
    private final LeaseSql leaseSql;

    private volatile Thread autoRenewThread;
    private volatile long autoRenewWindowMillis;
    private volatile PreparedStatement autoRenewStatement;

    LeaseLockInstance(String lockName, DataSource dataSource, String tableName,
                         Duration leaseDuration, Duration refreshInterval, Duration quietPeriod,
                         Duration pollInterval,
                         Consumer<FencepostException> onAutoRenewFailure,
                         String instanceId) {
        super(lockName, dataSource, tableName, LockType.LEASE);
        this.leaseDuration = leaseDuration;
        this.leaseDurationMs = leaseDuration.toMillis();
        this.refreshInterval = refreshInterval;
        this.quietPeriod = quietPeriod;
        this.quietPeriodMs = quietPeriod != null ? quietPeriod.toMillis() : 0;
        this.pollIntervalMs = pollInterval != null ? Durations.toPositiveMillis(pollInterval, "pollInterval") : DEFAULT_POLL_INTERVAL_MS;
        this.onAutoRenewFailure = onAutoRenewFailure;
        this.instanceId = instanceId;
        this.leaseSql = new LeaseSql(tableName);
    }

    static final class LeaseSql {
        final String acquire;
        final String renew;
        final String unlockWithQuietPeriod;
        final String unlock;

        LeaseSql(String tableName) {
            this.acquire = String.format("UPDATE %s SET token = token + 1, locked_by = ?, locked_at = now(), expires_at = now() + %s WHERE lock_name IN (SELECT lock_name FROM %s WHERE lock_name = ? AND (locked_by IS NULL OR expires_at IS NULL OR expires_at <= now()) FOR UPDATE SKIP LOCKED) RETURNING token", tableName, Jdbc.intervalMillis(), tableName);
            this.renew = String.format("UPDATE %s SET expires_at = GREATEST(expires_at, now() + %s) %s", tableName, Jdbc.intervalMillis(), activeLeasePredicate());
            this.unlockWithQuietPeriod = String.format("UPDATE %s SET token = token + 1, locked_at = NULL, expires_at = now() + %s %s", tableName, Jdbc.intervalMillis(), activeLeasePredicate());
            this.unlock = String.format("UPDATE %s SET token = token + 1, locked_by = NULL, locked_at = NULL, expires_at = NULL %s", tableName, activeLeasePredicate());
        }
    }

    @Override
    public FencingToken lock() {
        ensureNotHeld();
        return doLock();
    }

    @Override
    public FencingToken lock(Duration timeout) {
        Durations.requireAtLeastOneMillisecond(timeout, "timeout");
        ensureNotHeld();
        return doLock(timeout);
    }

    @Override
    public Optional<FencingToken> tryLock() {
        ensureNotHeld();
        return doTryLock();
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
            return Jdbc.query(dataSource, leaseSql.acquire)
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
            int updated = Jdbc.update(dataSource, leaseSql.renew)
                    .bind(durationMillis)
                    .bind(lockName)
                    .bind(token.value())
                    .execute();
            if (updated == 0) {
                invalidateCurrentToken(token.value());
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
                updated = Jdbc.update(dataSource, leaseSql.unlockWithQuietPeriod)
                        .bind(quietPeriodMs)
                        .bind(lockName)
                        .bind(token.value())
                        .execute();
            } else {
                updated = Jdbc.update(dataSource, leaseSql.unlock)
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
                } catch (SQLException e) {
                    logger.warn("auto-renew failed for lease lock '{}', token={}", lockName, token, e);
                    FencepostException ex = new FencepostException("Auto-renew failed for lock: " + lockName, e);
                    if (onAutoRenewFailure != null) {
                        onAutoRenewFailure.accept(ex);
                    }
                    return;
                }
            }
        });
        autoRenewThread.setDaemon(true);
        autoRenewThread.setName("fencepost-auto-renew-" + lockName);
        autoRenewThread.start();
    }

    private void refreshWithRetry(long windowMillis, long token) throws SQLException, InterruptedException {
        SQLException lastException = null;
        for (int attempt = 0; attempt < AUTO_RENEW_MAX_RETRIES; attempt++) {
            try {
                int updated = Jdbc.update(dataSource, leaseSql.renew)
                        .queryTimeout(refreshInterval)
                        .onStatement(ps -> autoRenewStatement = ps)
                        .bind(windowMillis)
                        .bind(lockName)
                        .bind(token)
                        .execute();
                if (updated == 0) {
                    throw new SQLException("Lock lost - token no longer matches or lease expired");
                }
                return;
            } catch (SQLException e) {
                lastException = e;
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException();
                }
                if (attempt < AUTO_RENEW_MAX_RETRIES - 1) {
                    Thread.sleep(100L * (attempt + 1));
                }
            } finally {
                autoRenewStatement = null;
            }
        }
        throw lastException;
    }

    private static String activeLeasePredicate() {
        return "WHERE lock_name = ? AND token = ? AND expires_at > now()";
    }

    private void invalidateCurrentToken(long token) {
        FencingToken held = currentToken;
        if (held != null && held.value() == token) {
            currentToken = null;
        }
    }

    private void stopAutoRenew() {
        Thread thread = autoRenewThread;
        if (thread == null) {
            return;
        }
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
