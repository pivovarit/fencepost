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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Session-scoped lock backed by PostgreSQL's {@code SELECT ... FOR UPDATE}.
 *
 * <p>Not thread-safe. Each instance should be used by a single thread at a time.
 * For concurrent access, create separate instances via {@code Factory.forName}.
 *
 * <p>Fencing tokens are allocated via a per-lock PostgreSQL sequence ({@code nextval}).
 * Because sequence increments are non-transactional, token values are visible to
 * {@link #isSuperseded} immediately — even while the lock-holding transaction is open —
 * and survive holder crashes. This requires only a single pooled connection per lock attempt.
 */
final class SessionLockInstance extends TableBasedLock implements FencedLock {

    private static final Logger logger = LoggerFactory.getLogger(SessionLockInstance.class);

    private final Sql sql;
    private final AtomicReference<Thread> owner = new AtomicReference<>();

    private volatile Connection connection;

    SessionLockInstance(String lockName, DataSource dataSource, String tableName) {
        super(lockName, dataSource, tableName, LockType.SESSION);
        this.sql = new Sql(tableName);
    }

    private static final class Sql {
        final String selectForUpdate;
        final String selectForUpdateSkipLocked;
        final String unlock;

        Sql(String tableName) {
            this.selectForUpdate = String.format("SELECT 1 FROM %s WHERE lock_name = ? FOR UPDATE", tableName);
            this.selectForUpdateSkipLocked = String.format("SELECT 1 FROM %s WHERE lock_name = ? FOR UPDATE SKIP LOCKED", tableName);
            this.unlock = String.format("UPDATE %s SET locked_by = NULL, locked_at = NULL WHERE lock_name = ?", tableName);
        }
    }

    @Override
    public FencingToken lock() {
        acquireOwnership();
        try {
            return doLock();
        } catch (Exception e) {
            releaseOwnership();
            throw e;
        }
    }

    @Override
    public FencingToken lock(Duration timeout) {
        Durations.requireAtLeastOneMillisecond(timeout, "timeout");
        acquireOwnership();
        try {
            return doLock(timeout);
        } catch (Exception e) {
            releaseOwnership();
            throw e;
        }
    }

    @Override
    public Optional<FencingToken> tryLock() {
        acquireOwnership();
        try {
            Optional<FencingToken> result = doTryLock();
            if (result.isEmpty()) {
                releaseOwnership();
            }
            return result;
        } catch (Exception e) {
            releaseOwnership();
            throw e;
        }
    }

    @Override
    FencingToken doLock() {
        try {
            connection = dataSource.getConnection();
            ensureRowExists(connection);
            connection.setAutoCommit(false);

            Jdbc.query(connection, sql.selectForUpdate)
                    .bind(lockName)
                    .map(ResultSet::next);

            String lockedBy = resolveLockedBy();
            currentToken = recordSessionToken(connection, allocateSessionToken(connection, lockedBy));
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
        try {
            connection = dataSource.getConnection();
            ensureRowExists(connection);
            connection.setAutoCommit(false);

            Jdbc.setStatementTimeout(connection, timeout);

            Jdbc.query(connection, sql.selectForUpdate)
                    .bind(lockName)
                    .map(ResultSet::next);

            Jdbc.resetStatementTimeout(connection);

            String lockedBy = resolveLockedBy();
            currentToken = recordSessionToken(connection, allocateSessionToken(connection, lockedBy));
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

            boolean locked = Jdbc.query(connection, sql.selectForUpdateSkipLocked)
                    .bind(lockName)
                    .map(ResultSet::next);

            if (!locked) {
                rollbackAndClose();
                logger.debug("tryLock failed for session lock '{}' - already held", lockName);
                return Optional.empty();
            }

            String lockedBy = resolveLockedBy();
            currentToken = recordSessionToken(connection, allocateSessionToken(connection, lockedBy));
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
        return checkSupersededBySessionSequence(token);
    }

    @Override
    public void unlock() {
        if (currentToken == null) {
            throw new LockNotHeldException(lockName);
        }
        long token = currentToken.value();
        try {
            Jdbc.update(connection, sql.unlock)
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
            releaseOwnership();
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

    private void acquireOwnership() {
        Thread current = Thread.currentThread();
        Thread existing = owner.compareAndExchange(null, current);
        if (existing != null) {
            if (existing == current) {
                throw new IllegalStateException("Lock already held by this thread: " + lockName);
            }
            throw new IllegalStateException(
                "SessionLockInstance is not thread-safe — already in use by thread '"
                    + existing.getName() + "', called from thread '" + current.getName()
                    + "'. Create separate instances via Factory.forName for concurrent access.");
        }
    }

    private void releaseOwnership() {
        owner.set(null);
    }

    private static String resolveLockedBy() {
        return HOSTNAME + "/" + Thread.currentThread().getName();
    }
}
