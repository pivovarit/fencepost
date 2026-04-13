package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.LockAcquisitionTimeoutException;
import com.pivovarit.fencepost.lock.LockNotHeldException;
import com.pivovarit.fencepost.lock.RenewableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;

final class LeaseLockInstance extends TableBasedLock implements RenewableLock {

    private static final Logger logger = LoggerFactory.getLogger(LeaseLockInstance.class);
    private static final long DEFAULT_POLL_INTERVAL_MS = 100;
    private static final int AUTO_RENEW_MAX_RETRIES = 3;

    private final Duration leaseDuration;
    private final Duration refreshInterval;
    private final Duration quietPeriod;
    private final long pollIntervalMs;
    private final Consumer<FencepostException> onAutoRenewFailure;

    private volatile Thread autoRenewThread;
    private volatile long autoRenewWindowMillis;

    LeaseLockInstance(String lockName, DataSource dataSource, String tableName,
                         Duration leaseDuration, Duration refreshInterval, Duration quietPeriod,
                         Duration pollInterval,
                         Consumer<FencepostException> onAutoRenewFailure) {
        super(lockName, dataSource, tableName);
        this.leaseDuration = leaseDuration;
        this.refreshInterval = refreshInterval;
        this.quietPeriod = quietPeriod;
        this.pollIntervalMs = pollInterval != null ? pollInterval.toMillis() : DEFAULT_POLL_INTERVAL_MS;
        this.onAutoRenewFailure = onAutoRenewFailure;
    }

    @Override
    public FencingToken lock() {
        ensureNotHeld();
        return doLock();
    }

    @Override
    public FencingToken lock(Duration timeout) {
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
            Optional<FencingToken> result = tryAcquireTimestamp();
            if (result.isPresent()) {
                currentToken = result.get();
                logger.debug("acquired lease lock '{}', token={}, lease={}", lockName, currentToken.value(), leaseDuration);
                if (hasAutoRenew()) {
                    startAutoRenew();
                }
                return currentToken;
            }

            if (timeout != null && System.nanoTime() >= deadlineNanos) {
                logger.debug("timed out acquiring lease lock '{}' after {}", lockName, timeout);
                throw new LockAcquisitionTimeoutException(lockName);
            }

            try {
                Thread.sleep(pollIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FencepostException("Interrupted while waiting for lock: " + lockName);
            }
        }
    }

    private Optional<FencingToken> tryAcquireTimestamp() {
        String lockedBy = String.format("%s/%s", HOSTNAME, Thread.currentThread().getName());

        String sql = String.format("UPDATE %s SET token = token + 1, locked_by = ?, locked_at = now(), expires_at = now() + %s WHERE lock_name = ? AND (locked_by IS NULL OR expires_at IS NULL OR expires_at <= now()) RETURNING token", tableName, Jdbc.intervalMillis());

        try {
            return Jdbc.query(dataSource, sql)
                    .bind(lockedBy)
                    .bind(leaseDuration.toMillis())
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
        if (duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException("duration must be positive");
        }
        if (currentToken == null) {
            throw new LockNotHeldException(lockName);
        }
        try {
            int updated = Jdbc.update(dataSource, String.format("UPDATE %s SET expires_at = GREATEST(expires_at, now() + %s) WHERE lock_name = ? AND token = ?", tableName, Jdbc.intervalMillis()))
                    .bind(duration.toMillis())
                    .bind(lockName)
                    .bind(currentToken.value())
                    .execute();
            if (updated == 0) {
                currentToken = null;
                throw new LockNotHeldException(lockName);
            }
            logger.debug("renewed lease lock '{}', token={}, duration={}", lockName, currentToken.value(), duration);
            if (autoRenewThread != null) {
                autoRenewWindowMillis = duration.toMillis();
            }
        } catch (LockNotHeldException e) {
            throw e;
        } catch (SQLException e) {
            throw new FencepostException("Failed to renew lock: " + lockName, e);
        }
    }

    @Override
    public void unlock() {
        if (currentToken == null) {
            throw new LockNotHeldException(lockName);
        }
        long token = currentToken.value();
        stopAutoRenew();
        try {
            int updated;
            if (quietPeriod != null) {
                updated = Jdbc.update(dataSource, String.format("UPDATE %s SET locked_at = NULL, expires_at = GREATEST(now(), locked_at + %s) WHERE lock_name = ? AND token = ?", tableName, Jdbc.intervalMillis()))
                        .bind(quietPeriod.toMillis())
                        .bind(lockName)
                        .bind(currentToken.value())
                        .execute();
            } else {
                updated = Jdbc.update(dataSource, String.format("UPDATE %s SET locked_by = NULL, locked_at = NULL, expires_at = NULL WHERE lock_name = ? AND token = ?", tableName))
                        .bind(lockName)
                        .bind(currentToken.value())
                        .execute();
            }
            if (updated == 0) {
                throw new LockNotHeldException(lockName);
            }
            logger.debug("released lease lock '{}', token={}", lockName, token);
        } catch (SQLException e) {
            throw new FencepostException("Failed to release lock: " + lockName, e);
        } finally {
            currentToken = null;
        }
    }

    @Override
    public void close() {
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
        autoRenewWindowMillis = leaseDuration.toMillis();
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
                int updated = Jdbc.update(dataSource, String.format("UPDATE %s SET expires_at = GREATEST(expires_at, now() + %s) WHERE lock_name = ? AND token = ?", tableName, Jdbc.intervalMillis()))
                        .bind(windowMillis)
                        .bind(lockName)
                        .bind(token)
                        .execute();
                if (updated == 0) {
                    throw new SQLException("Lock lost - token no longer matches");
                }
                return;
            } catch (SQLException e) {
                lastException = e;
                if (attempt < AUTO_RENEW_MAX_RETRIES - 1) {
                    Thread.sleep(100L * (attempt + 1));
                }
            }
        }
        throw lastException;
    }

    private void stopAutoRenew() {
        if (autoRenewThread != null) {
            autoRenewThread.interrupt();
            try {
                autoRenewThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            autoRenewThread = null;
        }
    }
}
