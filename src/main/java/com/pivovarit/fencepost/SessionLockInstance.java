package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

final class SessionLockInstance extends TableBasedLock implements FencedLock {

    private volatile Connection connection;

    SessionLockInstance(String lockName, DataSource dataSource, String tableName) {
        super(lockName, dataSource, tableName);
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
        try {
            connection = dataSource.getConnection();
            ensureRowExists(connection);
            connection.setAutoCommit(false);

            Jdbc.query(connection, "SELECT 1 FROM " + tableName + " WHERE lock_name = ? FOR UPDATE")
                    .bind(lockName)
                    .map(ResultSet::next);

            currentToken = incrementToken(connection, null);
            return currentToken;
        } catch (Exception e) {
            rollbackAndClose();
            throw (e instanceof FencepostException) ? (FencepostException) e
                : new FencepostException("Failed to acquire lock: " + lockName, e);
        }
    }

    @Override
    FencingToken doLock(Duration timeout) {
        try {
            connection = dataSource.getConnection();
            ensureRowExists(connection);
            connection.setAutoCommit(false);

            Jdbc.setStatementTimeout(connection, timeout);

            Jdbc.query(connection, "SELECT 1 FROM " + tableName + " WHERE lock_name = ? FOR UPDATE")
                    .bind(lockName)
                    .map(ResultSet::next);

            Jdbc.resetStatementTimeout(connection);

            currentToken = incrementToken(connection, null);
            return currentToken;
        } catch (Exception e) {
            rollbackAndClose();
            if (e instanceof SQLException && isStatementTimeout((SQLException) e)) {
                throw new LockAcquisitionTimeoutException(lockName);
            }
            throw (e instanceof FencepostException) ? (FencepostException) e
                : new FencepostException("Failed to acquire lock: " + lockName, e);
        }
    }

    @Override
    Optional<FencingToken> doTryLock() {
        try {
            connection = dataSource.getConnection();
            ensureRowExists(connection);
            connection.setAutoCommit(false);

            boolean locked = Jdbc.query(connection, "SELECT 1 FROM " + tableName + " WHERE lock_name = ? FOR UPDATE SKIP LOCKED")
                    .bind(lockName)
                    .map(ResultSet::next);

            if (!locked) {
                rollbackAndClose();
                return Optional.empty();
            }

            currentToken = incrementToken(connection, null);
            return Optional.of(currentToken);
        } catch (Exception e) {
            rollbackAndClose();
            throw (e instanceof FencepostException) ? (FencepostException) e
                : new FencepostException("Failed to try-lock: " + lockName, e);
        }
    }

    @Override
    public boolean isSuperseded(FencingToken token) {
        return checkSuperseded(token);
    }

    @Override
    public void unlock() {
        if (currentToken == null) {
            throw new LockNotHeldException(lockName);
        }
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

    @Override
    public void close() {
        if (currentToken != null) {
            try {
                unlock();
            } catch (Exception ignored) {
            }
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
}
