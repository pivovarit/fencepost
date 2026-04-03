package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;

final class ExpiringLockInstance extends TableBasedLock implements RenewableLock {

    private static final long POLL_INTERVAL_MS = 100;
    private static final int HEARTBEAT_MAX_RETRIES = 3;

    private final Duration expiryWindow;
    private final Duration refreshInterval;
    private final Duration quietPeriod;
    private final Consumer<FencepostException> onHeartbeatFailure;

    private volatile Thread heartbeatThread;
    private volatile long heartbeatWindowMillis;

    ExpiringLockInstance(String lockName, DataSource dataSource, String tableName,
                         Duration expiryWindow, Duration refreshInterval, Duration quietPeriod,
                         Consumer<FencepostException> onHeartbeatFailure) {
        super(lockName, dataSource, tableName);
        this.expiryWindow = expiryWindow;
        this.refreshInterval = refreshInterval;
        this.quietPeriod = quietPeriod;
        this.onHeartbeatFailure = onHeartbeatFailure;
    }

    @Override
    public void lock() {
        baseLock();
    }

    @Override
    public void lock(Duration timeout) {
        baseLock(timeout);
    }

    @Override
    public boolean tryLock() {
        return baseTryLock();
    }

    @Override
    public FencingToken fencedLock() {
        ensureNotHeld();
        return doFencedLock();
    }

    @Override
    public FencingToken fencedLock(Duration timeout) {
        ensureNotHeld();
        return doFencedLock(timeout);
    }

    @Override
    public Optional<FencingToken> tryFencedLock() {
        ensureNotHeld();
        return doTryFencedLock();
    }

    @Override
    FencingToken doFencedLock() {
        return lockTimestampBlocking(null);
    }

    @Override
    FencingToken doFencedLock(Duration timeout) {
        return lockTimestampBlocking(timeout);
    }

    @Override
    Optional<FencingToken> doTryFencedLock() {
        ensureRowExists();
        Optional<FencingToken> result = tryAcquireTimestamp();
        if (result.isPresent()) {
            currentToken = result.get();
            if (hasHeartbeat()) {
                startHeartbeat();
            }
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
                if (hasHeartbeat()) {
                    startHeartbeat();
                }
                return currentToken;
            }

            if (timeout != null && System.nanoTime() >= deadlineNanos) {
                throw new LockAcquisitionTimeoutException(lockName);
            }

            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FencepostException("Interrupted while waiting for lock: " + lockName);
            }
        }
    }

    private Optional<FencingToken> tryAcquireTimestamp() {
        String lockedBy = String.format("%s/%s", HOSTNAME, Thread.currentThread().getName());
        long millis = expiryWindow.toMillis();
        String expiresAtExpr = "now() + interval '" + millis + " milliseconds'";

        String sql = String.format(
                "UPDATE %s SET token = token + 1, locked_by = ?, locked_at = now(), expires_at = %s WHERE lock_name = ? AND (locked_by IS NULL OR expires_at IS NULL OR expires_at <= now()) RETURNING token",
                tableName, expiresAtExpr);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, lockedBy);
            ps.setString(2, lockName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(new FencingToken(rs.getLong(1))) : Optional.empty();
            }
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
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE " + tableName + " SET expires_at = now() + interval '" + duration.toMillis() + " milliseconds' " +
                     "WHERE lock_name = ? AND token = ?")) {
            ps.setString(1, lockName);
            ps.setLong(2, currentToken.value());
            int updated = ps.executeUpdate();
            if (updated == 0) {
                currentToken = null;
                throw new LockNotHeldException(lockName);
            }
            if (heartbeatThread != null) {
                heartbeatWindowMillis = duration.toMillis();
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
        stopHeartbeat();
        String expiresAtExpr = quietPeriod != null
            ? "GREATEST(now(), locked_at + interval '" + quietPeriod.toMillis() + " milliseconds')"
            : "NULL";
        String lockedByExpr = quietPeriod != null ? "locked_by" : "NULL";
        String lockedAtExpr = quietPeriod != null ? "locked_at" : "NULL";
        try (Connection unlockConn = dataSource.getConnection();
             PreparedStatement ps = unlockConn.prepareStatement(
                     "UPDATE " + tableName + " SET locked_by = " + lockedByExpr + ", locked_at = " + lockedAtExpr + ", expires_at = " + expiresAtExpr + " WHERE lock_name = ? AND token = ?")) {
            ps.setString(1, lockName);
            ps.setLong(2, currentToken.value());
            int updated = ps.executeUpdate();
            if (updated == 0) {
                throw new LockNotHeldException(lockName);
            }
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
            } catch (Exception ignored) {
            }
        }
    }

    private boolean hasHeartbeat() {
        return refreshInterval != null;
    }

    private void startHeartbeat() {
        long token = currentToken.value();
        heartbeatWindowMillis = expiryWindow.toMillis();
        heartbeatThread = new Thread(() -> {
            long intervalMillis = refreshInterval.toMillis();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(intervalMillis);
                    refreshWithRetry(heartbeatWindowMillis, token);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (SQLException e) {
                    currentToken = null;
                    FencepostException ex = new FencepostException("Heartbeat failed for lock: " + lockName, e);
                    if (onHeartbeatFailure != null) {
                        onHeartbeatFailure.accept(ex);
                    }
                    return;
                }
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.setName("fencepost-heartbeat-" + lockName);
        heartbeatThread.start();
    }

    private void refreshWithRetry(long windowMillis, long token) throws SQLException, InterruptedException {
        SQLException lastException = null;
        for (int attempt = 0; attempt < HEARTBEAT_MAX_RETRIES; attempt++) {
            try (Connection hbConn = dataSource.getConnection();
                 PreparedStatement ps = hbConn.prepareStatement(
                         String.format("UPDATE %s SET expires_at = now() + interval '%d milliseconds' WHERE lock_name = ? AND token = ?",
                                 tableName, windowMillis))) {
                ps.setString(1, lockName);
                ps.setLong(2, token);
                int updated = ps.executeUpdate();
                if (updated == 0) {
                    throw new SQLException("Lock lost — token no longer matches");
                }
                return;
            } catch (SQLException e) {
                lastException = e;
                if (attempt < HEARTBEAT_MAX_RETRIES - 1) {
                    Thread.sleep(100L * (attempt + 1));
                }
            }
        }
        throw lastException;
    }

    private void stopHeartbeat() {
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
            try {
                heartbeatThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            heartbeatThread = null;
        }
    }
}
