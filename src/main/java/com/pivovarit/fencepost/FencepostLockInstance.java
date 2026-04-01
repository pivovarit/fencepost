package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;

final class FencepostLockInstance implements FencepostLock {

    private static final long POLL_INTERVAL_MS = 100;

    private final String lockName;
    private final DataSource dataSource;
    private final LockMode lockMode;
    private final String tableName;
    private final Consumer<FencepostException> onHeartbeatFailure;

    volatile Connection connection;
    private volatile FencingToken currentToken;
    private volatile Thread heartbeatThread;
    private volatile Thread keepaliveThread;
    private volatile long heartbeatWindowMillis;

    FencepostLockInstance(String lockName, DataSource dataSource, LockMode lockMode, String tableName, Consumer<FencepostException> onHeartbeatFailure) {
        this.lockName = lockName;
        this.dataSource = dataSource;
        this.lockMode = lockMode;
        this.tableName = tableName;
        this.onHeartbeatFailure = onHeartbeatFailure;
    }

    private Duration quietPeriod() {
        return lockMode instanceof LockMode.Expiring ? ((LockMode.Expiring) lockMode).quietPeriod() : null;
    }

    private boolean isTimestampBased() {
        return lockMode instanceof LockMode.Expiring;
    }

    @Override
    public FencingToken lock() {
        ensureNotHeld();
        if (isTimestampBased()) {
            return lockTimestampBlocking(null);
        }
        return lockConnection();
    }

    @Override
    public FencingToken lock(Duration timeout) {
        ensureNotHeld();
        if (isTimestampBased()) {
            return lockTimestampBlocking(timeout);
        }
        return lockConnectionWithTimeout(timeout);
    }

    @Override
    public Optional<FencingToken> tryLock() {
        ensureNotHeld();
        if (isTimestampBased()) {
            return tryLockTimestamp();
        }
        return tryLockConnection();
    }

    private void ensureNotHeld() {
        if (currentToken != null) {
            throw new IllegalStateException("Lock already held: " + lockName);
        }
    }

    private FencingToken lockConnection() {
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);

            ensureRowExists();

            try (PreparedStatement ps = connection.prepareStatement(
              "SELECT 1 FROM " + tableName + " WHERE lock_name = ? FOR UPDATE")) {
                ps.setString(1, lockName);
                ps.executeQuery();
            }

            currentToken = incrementToken(connection, "NULL");
            if (lockMode instanceof LockMode.Connection && ((LockMode.Connection) lockMode).hasKeepalive()) {
                startKeepalive((LockMode.Connection) lockMode);
            }
            return currentToken;
        } catch (Exception e) {
            rollbackAndClose();
            throw (e instanceof FencepostException) ? (FencepostException) e : new FencepostException("Failed to acquire lock: " + lockName, e);
        }
    }

    private FencingToken lockConnectionWithTimeout(Duration timeout) {
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);

            ensureRowExists();

            try (PreparedStatement ps = connection.prepareStatement(
              "SET LOCAL statement_timeout = '" + timeout.toMillis() + "ms'")) {
                ps.execute();
            }

            try (PreparedStatement ps = connection.prepareStatement(
              "SELECT 1 FROM " + tableName + " WHERE lock_name = ? FOR UPDATE")) {
                ps.setString(1, lockName);
                ps.executeQuery();
            }

            try (PreparedStatement ps = connection.prepareStatement(
              "SET LOCAL statement_timeout = 0")) {
                ps.execute();
            }

            currentToken = incrementToken(connection, "NULL");
            if (lockMode instanceof LockMode.Connection && ((LockMode.Connection) lockMode).hasKeepalive()) {
                startKeepalive((LockMode.Connection) lockMode);
            }
            return currentToken;
        } catch (Exception e) {
            rollbackAndClose();
            if (e instanceof SQLException && isStatementTimeout((SQLException) e)) {
                throw new LockAcquisitionTimeoutException(lockName);
            }
            throw (e instanceof FencepostException) ? (FencepostException) e : new FencepostException("Failed to acquire lock: " + lockName, e);
        }
    }

    private Optional<FencingToken> tryLockConnection() {
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);

            ensureRowExists();

            try (PreparedStatement ps = connection.prepareStatement(
              "SELECT 1 FROM " + tableName + " WHERE lock_name = ? FOR UPDATE SKIP LOCKED")) {
                ps.setString(1, lockName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        rollbackAndClose();
                        return Optional.empty();
                    }
                }
            }

            currentToken = incrementToken(connection, "NULL");
            if (lockMode instanceof LockMode.Connection && ((LockMode.Connection) lockMode).hasKeepalive()) {
                startKeepalive((LockMode.Connection) lockMode);
            }
            return Optional.of(currentToken);
        } catch (Exception e) {
            rollbackAndClose();
            throw (e instanceof FencepostException) ? (FencepostException) e : new FencepostException("Failed to try-lock: " + lockName, e);
        }
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
                LockMode.Expiring expiring = (LockMode.Expiring) lockMode;
                if (expiring.hasHeartbeat()) {
                    startHeartbeat(expiring);
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

    private Optional<FencingToken> tryLockTimestamp() {
        ensureRowExists();

        Optional<FencingToken> result = tryAcquireTimestamp();
        if (result.isPresent()) {
            currentToken = result.get();
            LockMode.Expiring exp = (LockMode.Expiring) lockMode;
            if (exp.hasHeartbeat()) {
                startHeartbeat(exp);
            }
        }
        return result;
    }

    private Optional<FencingToken> tryAcquireTimestamp() {
        String lockedBy = String.format("%s/%s", HOSTNAME, Thread.currentThread().getName());
        String expiresAtExpr = expiresAtExpression();

        String sql = String.format("UPDATE %s SET token = token + 1, locked_by = ?, locked_at = now(), expires_at = %s WHERE lock_name = ? AND (locked_by IS NULL OR expires_at IS NULL OR expires_at <= now()) RETURNING token", tableName, expiresAtExpr);

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
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT token > ? FROM " + tableName + " WHERE lock_name = ?")) {
            ps.setLong(1, token.value());
            ps.setString(2, lockName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new FencepostException("Lock row not found: " + lockName);
                }
                return rs.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new FencepostException("Failed to check token for lock: " + lockName, e);
        }
    }

    @Override
    public void withLock(ThrowingConsumer<FencingToken> action) {
        FencingToken token = lock();
        try {
            action.accept(token);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FencepostException("Action failed while holding lock: " + lockName, e);
        } finally {
            unlock();
        }
    }

    @Override
    public void withLock(Duration timeout, ThrowingConsumer<FencingToken> action) {
        FencingToken token = lock(timeout);
        try {
            action.accept(token);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FencepostException("Action failed while holding lock: " + lockName, e);
        } finally {
            unlock();
        }
    }

    @Override
    public void renew(Duration duration) {
        if (duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException("duration must be positive");
        }
        if (currentToken == null) {
            throw new LockNotHeldException(lockName);
        }
        if (!isTimestampBased()) {
            throw new UnsupportedOperationException("renew() is only supported in expiring lock mode");
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
        Duration quiet = quietPeriod();
        if (isTimestampBased()) {
            String expiresAtExpr = quiet != null
              ? "GREATEST(now(), locked_at + interval '" + quiet.toMillis() + " milliseconds')"
              : "NULL";
            String lockedByExpr = quiet != null ? "locked_by" : "NULL";
            String lockedAtExpr = quiet != null ? "locked_at" : "NULL";
            try (Connection unlockConn = dataSource.getConnection();
                 PreparedStatement ps = unlockConn.prepareStatement("UPDATE " + tableName + " SET locked_by = " + lockedByExpr + ", locked_at = " + lockedAtExpr + ", expires_at = " + expiresAtExpr + " WHERE lock_name = ? AND token = ?")) {
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
        } else {
            stopKeepalive();
            try {
                connection.commit();
            } catch (SQLException e) {
                try {
                    connection.rollback();
                } catch (SQLException ignored) {
                }
                throw new FencepostException("Failed to release lock: " + lockName, e);
            } finally {
                closeConnection();
                connection = null;
                currentToken = null;
            }
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
        stopKeepalive();
    }

    private void ensureRowExists() {
        try (Connection autoCommitConn = dataSource.getConnection()) {
            autoCommitConn.setAutoCommit(true);
            boolean exists;
            try (PreparedStatement ps = autoCommitConn.prepareStatement(String.format("SELECT 1 FROM %s WHERE lock_name = ?", tableName))) {
                ps.setString(1, lockName);
                try (ResultSet rs = ps.executeQuery()) {
                    exists = rs.next();
                }
            }
            if (!exists) {
                try (PreparedStatement ps = autoCommitConn.prepareStatement(
                  "INSERT INTO " + tableName + " (lock_name) VALUES (?) ON CONFLICT DO NOTHING")) {
                    ps.setString(1, lockName);
                    ps.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new FencepostException("Failed to ensure lock row exists: " + lockName, e);
        }
    }

    private FencingToken incrementToken(Connection conn, String expiresAtExpr) throws SQLException {
        String lockedBy = HOSTNAME + "/" + Thread.currentThread().getName();

        String sql = String.format("UPDATE %s SET token = token + 1, locked_by = ?, locked_at = now(), expires_at = %s WHERE lock_name = ? RETURNING token", tableName, expiresAtExpr);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, lockedBy);
            ps.setString(2, lockName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return new FencingToken(rs.getLong(1));
            }
        }
    }

    private String expiresAtExpression() {
        if (lockMode instanceof LockMode.Expiring) {
            long millis = ((LockMode.Expiring) lockMode).expiryWindow().toMillis();
            return "now() + interval '" + millis + " milliseconds'";
        }
        return "NULL";
    }

    private static final int HEARTBEAT_MAX_RETRIES = 3;

    private void startHeartbeat(LockMode.Expiring strategy) {
        long token = currentToken.value();
        heartbeatWindowMillis = strategy.expiryWindow().toMillis();
        heartbeatThread = new Thread(() -> {
            long intervalMillis = strategy.refreshInterval().toMillis();
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
                 PreparedStatement ps = hbConn.prepareStatement(String.format("UPDATE %s SET expires_at = now() + interval '%d milliseconds' WHERE lock_name = ? AND token = ?", tableName, windowMillis))) {
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

    private void startKeepalive(LockMode.Connection connMode) {
        keepaliveThread = new Thread(() -> {
            long intervalMillis = connMode.keepaliveInterval().toMillis();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(intervalMillis);
                    try (PreparedStatement ps = connection.prepareStatement("SELECT 1")) {
                        ps.execute();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (SQLException e) {
                    currentToken = null;
                    FencepostException ex = new FencepostException("Keepalive failed for lock: " + lockName, e);
                    if (onHeartbeatFailure != null) {
                        onHeartbeatFailure.accept(ex);
                    }
                    return;
                }
            }
        });
        keepaliveThread.setDaemon(true);
        keepaliveThread.setName("fencepost-keepalive-" + lockName);
        keepaliveThread.start();
    }

    private void stopKeepalive() {
        if (keepaliveThread != null) {
            keepaliveThread.interrupt();
            try {
                keepaliveThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            keepaliveThread = null;
        }
    }

    private void rollbackAndClose() {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException ignored) {
            }
            closeConnection();
            connection = null;
        }
    }

    private void closeConnection() {
        try {
            connection.close();
        } catch (SQLException ignored) {
        }
    }

    private static boolean isStatementTimeout(SQLException e) {
        return SqlStates.QUERY_CANCELLED.equals(e.getSQLState());
    }

    private static final String HOSTNAME = resolveHostname();

    private static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
