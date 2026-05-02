package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.FencedLock;
import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.LockAcquisitionTimeoutException;
import com.pivovarit.fencepost.lock.LockNotHeldException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

/**
 * Session-scoped lock backed by PostgreSQL's {@code SELECT ... FOR UPDATE}.
 *
 * <p>Not thread-safe. Each instance should be used by a single thread at a time.
 * For concurrent access, create separate instances via {@code Factory.forName}.
 *
 * <p>Fencing tokens are allocated from a separate durable token table after the row lock is
 * acquired, so token increments survive holder crashes even though the row-lock transaction is
 * rolled back.
 *
 * <p><b>Pool sizing:</b> each lock attempt briefly needs a second pooled connection for token
 * allocation while the row lock connection is held. The connection pool must have headroom
 * beyond the number of concurrent lock waiters.
 */
final class SessionLockInstance extends TableBasedLock implements FencedLock {

    private static final Logger logger = LoggerFactory.getLogger(SessionLockInstance.class);

    private final String selectForUpdate;
    private final String selectForUpdateSkipLocked;

    private volatile Connection connection;

    SessionLockInstance(String lockName, DataSource dataSource, String tableName) {
        super(lockName, dataSource, tableName, LockType.SESSION);
        this.selectForUpdate = "SELECT 1 FROM " + tableName + " WHERE lock_name = ? FOR UPDATE";
        this.selectForUpdateSkipLocked = "SELECT 1 FROM " + tableName + " WHERE lock_name = ? FOR UPDATE SKIP LOCKED";
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
        try {
            connection = dataSource.getConnection();
            ensureRowExists(connection);
            connection.setAutoCommit(false);

            Jdbc.query(connection, selectForUpdate)
                    .bind(lockName)
                    .map(ResultSet::next);

            String lockedBy = resolveLockedBy();
            currentToken = recordSessionToken(connection, allocateSessionToken(lockedBy, Long.MAX_VALUE));
            logger.debug("acquired session lock '{}', token={}", lockName, currentToken.value());
            return currentToken;
        } catch (Exception e) {
            rollbackAndClose();
            logger.debug("failed to acquire session lock '{}'", lockName, e);
            throw (e instanceof FencepostException) ? (FencepostException) e
                : new FencepostException("Failed to acquire lock: " + lockName, e);
        }
    }

    @Override
    FencingToken doLock(Duration timeout) {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        try {
            connection = dataSource.getConnection();
            ensureRowExists(connection);
            connection.setAutoCommit(false);

            Jdbc.setStatementTimeout(connection, timeout);

            Jdbc.query(connection, selectForUpdate)
                    .bind(lockName)
                    .map(ResultSet::next);

            Jdbc.resetStatementTimeout(connection);

            String lockedBy = resolveLockedBy();
            currentToken = recordSessionToken(connection, allocateSessionToken(lockedBy, deadlineNanos));
            logger.debug("acquired session lock '{}', token={}", lockName, currentToken.value());
            return currentToken;
        } catch (Exception e) {
            rollbackAndClose();
            if (e instanceof SQLException && isStatementTimeout((SQLException) e)) {
                logger.debug("timed out acquiring session lock '{}' after {}", lockName, timeout);
                throw new LockAcquisitionTimeoutException(lockName);
            }
            logger.debug("failed to acquire session lock '{}'", lockName, e);
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

            boolean locked = Jdbc.query(connection, selectForUpdateSkipLocked)
                    .bind(lockName)
                    .map(ResultSet::next);

            if (!locked) {
                rollbackAndClose();
                logger.debug("tryLock failed for session lock '{}' - already held", lockName);
                return Optional.empty();
            }

            String lockedBy = resolveLockedBy();
            currentToken = recordSessionToken(connection, allocateSessionToken(lockedBy, Long.MAX_VALUE));
            logger.debug("acquired session lock '{}' via tryLock, token={}", lockName, currentToken.value());
            return Optional.of(currentToken);
        } catch (Exception e) {
            rollbackAndClose();
            logger.debug("failed to tryLock session lock '{}'", lockName, e);
            throw (e instanceof FencepostException) ? (FencepostException) e
                : new FencepostException("Failed to try-lock: " + lockName, e);
        }
    }

    @Override
    public boolean isSuperseded(FencingToken token) {
        return checkSupersededByTokenTable(token);
    }

    @Override
    public void unlock() {
        if (currentToken == null) {
            throw new LockNotHeldException(lockName);
        }
        long token = currentToken.value();
        try {
            Jdbc.update(connection, sql.unlockSession)
                .bind(lockName)
                .execute();
            connection.commit();
            logger.debug("released session lock '{}', token={}", lockName, token);
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.trace("failed to rollback after release failure for session lock '{}'", lockName, ex);
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
            } catch (Exception e) {
                logger.trace("failed to unlock session lock '{}' during close", lockName, e);
            }
        }
    }

    private void rollbackAndClose() {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                logger.trace("failed to rollback session lock '{}' connection", lockName, e);
            }
            closeConnection();
            connection = null;
        }
    }

    private void closeConnection() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.trace("failed to close session lock '{}' connection", lockName, e);
        }
    }

    private static String resolveLockedBy() {
        return HOSTNAME + "/" + Thread.currentThread().getName();
    }
}
