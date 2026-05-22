package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.FencingToken;

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
    final LockType lockType;
    final Sql sql;
    final String sessionTokenSeqName;
    final String allocateSessionTokenSql;

    volatile FencingToken currentToken;

    private volatile boolean rowExists;

    TableBasedLock(String lockName, DataSource dataSource, String tableName, LockType lockType) {
        this.lockName = lockName;
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.lockType = lockType;
        this.sql = new Sql(tableName);
        this.sessionTokenSeqName = lockType == LockType.SESSION
            ? computeSessionTokenSeqName(tableName, lockName)
            : null;
        this.allocateSessionTokenSql = sessionTokenSeqName != null
            ? "SELECT nextval('" + sessionTokenSeqName + "')"
            : null;
    }

    static final class Sql {
        final String selectLockType;
        final String insertLockRow;
        final String allocateToken;
        final String recordToken;
        final String recordTokenMetadata;
        final String checkSuperseded;
        final String checkSupersededByTokenTable;

        Sql(String tableName) {
            String tokenTable = tokenTableName(tableName);
            String tokenSequence = tokenSequenceName(tableName);
            this.selectLockType = String.format("SELECT lock_type FROM %s WHERE lock_name = ?", tableName);
            this.insertLockRow = String.format("INSERT INTO %s (lock_name, lock_type) VALUES (?, ?) ON CONFLICT DO NOTHING", tableName);
            this.allocateToken = """
                INSERT INTO %s AS t (lock_name, token, last_locked_by, last_locked_at)
                VALUES (?, nextval('%s'), ?, now())
                ON CONFLICT (lock_name) DO UPDATE
                SET token = EXCLUDED.token, last_locked_by = EXCLUDED.last_locked_by, last_locked_at = EXCLUDED.last_locked_at
                RETURNING token""".formatted(tokenTable, tokenSequence);
            this.recordToken = """
                UPDATE %s SET token = ?, locked_by = ?, locked_at = now(), expires_at = NULL
                WHERE lock_name = ? RETURNING token""".formatted(tableName);
            this.recordTokenMetadata = """
                INSERT INTO %s (lock_name, token, last_locked_by, last_locked_at)
                VALUES (?, ?, ?, now())
                ON CONFLICT (lock_name) DO UPDATE
                SET token = EXCLUDED.token, last_locked_by = EXCLUDED.last_locked_by, last_locked_at = EXCLUDED.last_locked_at""".formatted(tokenTable);
            this.checkSuperseded = String.format("SELECT token > ? FROM %s WHERE lock_name = ?", tableName);
            this.checkSupersededByTokenTable = String.format("SELECT token > ? FROM %s WHERE lock_name = ?", tokenTable);
        }
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
        try (Connection conn = dataSource.getConnection()) {
            ensureRowExists(conn);
        } catch (SQLException e) {
            throw new FencepostException("Failed to ensure lock row exists: " + lockName, e);
        }
    }

    void ensureRowExists(Connection conn) throws SQLException {
        if (rowExists) {
            return;
        }
        String type = lockType.name();
        conn.setAutoCommit(true);
        String storedType = Jdbc.query(conn, sql.selectLockType)
                .bind(lockName)
                .map(rs -> rs.next() ? rs.getString(1) : null);
        if (storedType == null) {
            Jdbc.update(conn, sql.insertLockRow)
                    .bind(lockName)
                    .bind(type)
                    .execute();
            storedType = Jdbc.query(conn, sql.selectLockType)
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
        if (lockType == LockType.SESSION) {
            ensureSessionSequenceExists(conn);
        }
        rowExists = true;
    }

    private void ensureSessionSequenceExists(Connection conn) throws SQLException {
        try {
            Jdbc.execute(conn, String.format("CREATE SEQUENCE IF NOT EXISTS %s", sessionTokenSeqName));
        } catch (SQLException e) {
            if (!SqlStates.UNIQUE_VIOLATION.equals(e.getSQLState())) {
                throw e;
            }
        }
    }

    FencingToken allocateSessionToken(Connection conn, String lockedBy) throws SQLException {
        long token = Jdbc.query(conn, allocateSessionTokenSql)
            .map(rs -> {
                rs.next();
                return rs.getLong(1);
            });
        Jdbc.update(conn, sql.recordTokenMetadata)
            .bind(lockName)
            .bind(token)
            .bind(lockedBy)
            .execute();
        return new FencingToken(token);
    }


    FencingToken recordSessionToken(Connection conn, FencingToken token) throws SQLException {
        String lockedBy = HOSTNAME + "/" + Thread.currentThread().getName();
        return Jdbc.query(conn, sql.recordToken)
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
            return Jdbc.query(dataSource, sql.checkSuperseded)
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

    boolean checkSupersededBySessionSequence(FencingToken token) {
        try {
            return Jdbc.query(dataSource,
                    String.format("SELECT is_called AND last_value > ? FROM %s", sessionTokenSeqName))
                .bind(token.value())
                .map(rs -> {
                    if (!rs.next()) {
                        throw new FencepostException("Session token sequence not found: " + lockName);
                    }
                    return rs.getBoolean(1);
                });
        } catch (SQLException e) {
            throw new FencepostException("Failed to check token for lock: " + lockName, e);
        }
    }

    static String computeSessionTokenSeqName(String tableName, String lockName) {
        String seqName = "fencepost_st_" + lockName.replace('-', '_');
        int dot = tableName.lastIndexOf('.');
        if (dot == -1) {
            return seqName;
        }
        return tableName.substring(0, dot + 1) + seqName;
    }

    static String tokenTableName(String tableName) {
        int dot = tableName.lastIndexOf('.');
        if (dot == -1) {
            return tableName + "_tokens";
        }
        return tableName.substring(0, dot + 1) + tableName.substring(dot + 1) + "_tokens";
    }

    static String tokenSequenceName(String tableName) {
        int dot = tableName.lastIndexOf('.');
        if (dot == -1) {
            return tableName + "_token_seq";
        }
        return tableName.substring(0, dot + 1) + tableName.substring(dot + 1) + "_token_seq";
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
