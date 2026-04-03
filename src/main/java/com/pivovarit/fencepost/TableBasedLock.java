package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
        try (Connection autoCommitConn = dataSource.getConnection()) {
            autoCommitConn.setAutoCommit(true);
            boolean exists;
            try (PreparedStatement ps = autoCommitConn.prepareStatement(
                    String.format("SELECT 1 FROM %s WHERE lock_name = ?", tableName))) {
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

    FencingToken incrementToken(Connection conn, String expiresAtExpr) throws SQLException {
        String lockedBy = HOSTNAME + "/" + Thread.currentThread().getName();
        String sql = String.format(
                "UPDATE %s SET token = token + 1, locked_by = ?, locked_at = now(), expires_at = %s WHERE lock_name = ? RETURNING token",
                tableName, expiresAtExpr);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, lockedBy);
            ps.setString(2, lockName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return new FencingToken(rs.getLong(1));
            }
        }
    }

    boolean checkSuperseded(FencingToken token) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT token > ? FROM " + tableName + " WHERE lock_name = ?")) {
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

    void baseLock() {
        ensureNotHeld();
        doFencedLock();
    }

    void baseLock(Duration timeout) {
        ensureNotHeld();
        doFencedLock(timeout);
    }

    boolean baseTryLock() {
        ensureNotHeld();
        return doTryFencedLock().isPresent();
    }

    abstract FencingToken doFencedLock();
    abstract FencingToken doFencedLock(Duration timeout);
    abstract Optional<FencingToken> doTryFencedLock();

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
