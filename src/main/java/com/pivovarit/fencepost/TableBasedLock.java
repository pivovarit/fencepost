package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.FencingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

abstract class TableBasedLock {

    private static final Logger logger = LoggerFactory.getLogger(TableBasedLock.class);

    static final String HOSTNAME = resolveHostname();

    final String lockName;
    final DataSource dataSource;
    final String tableName;
    final String tokenTableName;
    final LockType lockType;

    volatile FencingToken currentToken;

    private volatile boolean rowExists;

    TableBasedLock(String lockName, DataSource dataSource, String tableName, LockType lockType) {
        this.lockName = lockName;
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.tokenTableName = tokenTableName(tableName);
        this.lockType = lockType;
    }

    void ensureNotHeld() {
        if (currentToken != null) {
            throw new IllegalStateException("Lock already held: " + lockName);
        }
    }

    void ensureRowExists() {
        if (rowExists) {
            return;
        }
        String type = lockType.name();
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(true);
            String storedType = Jdbc.query(conn, String.format("SELECT lock_type FROM %s WHERE lock_name = ?", tableName))
                    .bind(lockName)
                    .map(rs -> rs.next() ? rs.getString(1) : null);
            if (storedType == null) {
                Jdbc.update(conn, String.format("INSERT INTO %s (lock_name, lock_type) VALUES (?, ?) ON CONFLICT DO NOTHING", tableName))
                        .bind(lockName)
                        .bind(type)
                        .execute();
                storedType = Jdbc.query(conn, String.format("SELECT lock_type FROM %s WHERE lock_name = ?", tableName))
                        .bind(lockName)
                        .map(rs -> {
                            rs.next();
                            return rs.getString(1);
                        });
            }
            if (!type.equals(storedType)) {
                throw new FencepostException(
                    String.format("Lock '%s' is already registered as %s, cannot use as %s", lockName, storedType, type));
            }
            rowExists = true;
        } catch (FencepostException e) {
            throw e;
        } catch (SQLException e) {
            throw new FencepostException("Failed to ensure lock row exists: " + lockName, e);
        }
    }

    static final int TOKEN_ALLOCATION_NETWORK_TIMEOUT_MS = 2000;

    FencingToken allocateSessionToken(String lockedBy, long deadlineNanos) throws SQLException {
        String sql = String.format(
            "INSERT INTO %s AS t (lock_name, token, last_locked_by, last_locked_at) VALUES (?, 1, ?, now()) " +
            "ON CONFLICT (lock_name) DO UPDATE SET token = t.token + 1, last_locked_by = EXCLUDED.last_locked_by, last_locked_at = now() RETURNING token",
            tokenTableName);

        int maxAttempts = 3;
        long backoffMs = 50;
        SQLException lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            int networkTimeoutMs = TOKEN_ALLOCATION_NETWORK_TIMEOUT_MS;
            if (deadlineNanos != Long.MAX_VALUE) {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
                if (remainingMs <= 0) {
                    throw new SQLException("Token allocation deadline exceeded for lock: " + lockName);
                }
                networkTimeoutMs = (int) Math.min(networkTimeoutMs, remainingMs);
            }
            try (Connection conn = dataSource.getConnection()) {
                int savedNetworkTimeout = conn.getNetworkTimeout();
                conn.setNetworkTimeout(Runnable::run, Math.max(networkTimeoutMs, 1));
                try {
                    return Jdbc.query(conn, sql)
                        .bind(lockName)
                        .bind(lockedBy)
                        .map(rs -> {
                            rs.next();
                            return new FencingToken(rs.getLong(1));
                        });
                } finally {
                    conn.setNetworkTimeout(Runnable::run, savedNetworkTimeout);
                }
            } catch (SQLException e) {
                lastException = e;
                if (attempt < maxAttempts) {
                    long sleepMs = backoffMs * attempt;
                    if (deadlineNanos != Long.MAX_VALUE) {
                        long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
                        sleepMs = Math.min(sleepMs, remainingMs);
                        if (sleepMs <= 0) break;
                    }
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Interrupted while allocating fencing token for lock: " + lockName, e);
                    }
                }
            }
        }
        throw new SQLException("Failed to allocate fencing token for lock '" + lockName +
            "' after " + maxAttempts + " attempts — connection pool may be exhausted", lastException);
    }


    FencingToken recordSessionToken(Connection conn, FencingToken token) throws SQLException {
        String lockedBy = HOSTNAME + "/" + Thread.currentThread().getName();
        return Jdbc.query(conn, String.format("UPDATE %s SET token = ?, locked_by = ?, locked_at = now(), expires_at = NULL WHERE lock_name = ? RETURNING token", tableName))
                .bind(token.value())
                .bind(lockedBy)
                .bind(lockName)
                .map(rs -> {
                    rs.next();
                    return new FencingToken(rs.getLong(1));
                });
    }

    boolean checkSuperseded(FencingToken token) {
        try {
            return Jdbc.query(dataSource, String.format("SELECT token > ? FROM %s WHERE lock_name = ?", tableName))
                    .bind(token.value())
                    .bind(lockName)
                    .map(rs -> {
                        if (!rs.next()) {
                            throw new FencepostException("Lock row not found: " + lockName);
                        }
                        return rs.getBoolean(1);
                    });
        } catch (SQLException e) {
            throw new FencepostException("Failed to check token for lock: " + lockName, e);
        }
    }

    boolean checkSupersededByTokenTable(FencingToken token) {
        try {
            return Jdbc.query(dataSource, String.format("SELECT token > ? FROM %s WHERE lock_name = ?", tokenTableName))
                    .bind(token.value())
                    .bind(lockName)
                    .map(rs -> {
                        if (!rs.next()) {
                            throw new FencepostException("Lock token row not found: " + lockName);
                        }
                        return rs.getBoolean(1);
                    });
        } catch (SQLException e) {
            throw new FencepostException("Failed to check token for lock: " + lockName, e);
        }
    }

    private static String tokenTableName(String tableName) {
        int dot = tableName.lastIndexOf('.');
        if (dot == -1) {
            return tableName + "_tokens";
        }
        return tableName.substring(0, dot + 1) + tableName.substring(dot + 1) + "_tokens";
    }

    abstract FencingToken doLock();
    abstract FencingToken doLock(Duration timeout);
    abstract Optional<FencingToken> doTryLock();

    static boolean isStatementTimeout(SQLException e) {
        return SqlStates.QUERY_CANCELLED.equals(e.getSQLState());
    }

    static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
