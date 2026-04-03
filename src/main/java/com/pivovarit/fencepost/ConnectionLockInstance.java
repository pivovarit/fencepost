package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

final class ConnectionLockInstance extends TableBasedLock implements FencedLock {

    private volatile Connection connection;

    ConnectionLockInstance(String lockName, DataSource dataSource, String tableName) {
        super(lockName, dataSource, tableName);
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
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            ensureRowExists();

            Jdbc.query(connection,
                    "SELECT 1 FROM " + tableName + " WHERE lock_name = ? FOR UPDATE",
              ResultSet::next, lockName);

            currentToken = incrementToken(connection, null);
            return currentToken;
        } catch (Exception e) {
            rollbackAndClose();
            throw (e instanceof FencepostException) ? (FencepostException) e
                : new FencepostException("Failed to acquire lock: " + lockName, e);
        }
    }

    @Override
    FencingToken doFencedLock(Duration timeout) {
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            ensureRowExists();

            Jdbc.setStatementTimeout(connection, timeout);

            Jdbc.query(connection,"SELECT 1 FROM " + tableName + " WHERE lock_name = ? FOR UPDATE", ResultSet::next, lockName);

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
    Optional<FencingToken> doTryFencedLock() {
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            ensureRowExists();

            boolean locked = Jdbc.query(connection,"SELECT 1 FROM " + tableName + " WHERE lock_name = ? FOR UPDATE SKIP LOCKED", ResultSet::next, lockName);

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
