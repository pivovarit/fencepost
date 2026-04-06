package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

abstract class TableBasedLock {

    static final String HOSTNAME = resolveHostname();

    final String lockName;
    final DataSource dataSource;
    final String tableName;

    volatile FencingToken currentToken;

    TableBasedLock(String lockName, DataSource dataSource, String tableName) {
        this.lockName = lockName;
        this.dataSource = dataSource;
        this.tableName = tableName;
    }

    void ensureNotHeld() {
        if (currentToken != null) {
            throw new IllegalStateException("Lock already held: " + lockName);
        }
    }

    void ensureRowExists() {
        try {
            Jdbc.update(dataSource, "INSERT INTO " + tableName + " (lock_name) VALUES (?) ON CONFLICT DO NOTHING")
                    .bind(lockName)
                    .execute();
        } catch (SQLException e) {
            throw new FencepostException("Failed to ensure lock row exists: " + lockName, e);
        }
    }

    FencingToken incrementToken(Connection conn, Duration expiry) throws SQLException {
        String lockedBy = HOSTNAME + "/" + Thread.currentThread().getName();
        if (expiry != null) {
            return Jdbc.query(conn, String.format("UPDATE %s SET token = token + 1, locked_by = ?, locked_at = now(), expires_at = now() + %s WHERE lock_name = ? RETURNING token", tableName, Jdbc.intervalMillis()))
                    .bind(lockedBy)
                    .bind(expiry.toMillis())
                    .bind(lockName)
                    .map(rs -> {
                        rs.next();
                        return new FencingToken(rs.getLong(1));
                    });
        }
        return Jdbc.query(conn, String.format("UPDATE %s SET token = token + 1, locked_by = ?, locked_at = now(), expires_at = NULL WHERE lock_name = ? RETURNING token", tableName))
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

    abstract FencingToken doLock();
    abstract FencingToken doLock(Duration timeout);
    abstract Optional<FencingToken> doTryLock();

    static boolean isStatementTimeout(SQLException e) {
        return SqlStates.QUERY_CANCELLED.equals(e.getSQLState());
    }

    private static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
