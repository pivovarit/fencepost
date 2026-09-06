package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.AdvisoryLock;
import com.pivovarit.fencepost.lock.LockAcquisitionTimeoutException;
import com.pivovarit.fencepost.lock.LockNotHeldException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.Executor;

/**
 * Not thread-safe. Each instance should be used by a single thread at a time.
 * For concurrent access, use separate {@link AdvisoryLock} instances.
 */
final class AdvisoryLockInstance implements AdvisoryLock {

    private static final Logger logger = LoggerFactory.getLogger(AdvisoryLockInstance.class);
    private static final String ADVISORY_NAMESPACE = "fencepost:";

    private final String lockName;
    private final long advisoryKey;
    private final DataSource dataSource;
    private final Sql sql;

    private Connection connection;
    private boolean held;

    AdvisoryLockInstance(String lockName, DataSource dataSource) {
        this.lockName = lockName;
        this.advisoryKey = HashUtils.fnv1a64(ADVISORY_NAMESPACE + lockName);
        this.dataSource = dataSource;
        this.sql = new Sql();
    }

    private static final class Sql {
        final String lock = "SELECT pg_advisory_lock(?)";
        final String tryLock = "SELECT pg_try_advisory_lock(?)";
        final String unlock = "SELECT pg_advisory_unlock(?)";
        final String unlockAll = "SELECT pg_advisory_unlock_all()";
    }

    @Override
    public void lock() {
        ensureNotHeld();
        try {
            connection = dataSource.getConnection();
            Jdbc.query(connection, sql.lock)
              .bind(advisoryKey)
              .map(ResultSet::next);
            commitIfInTransaction();
            held = true;
            logger.debug("acquired advisory lock '{}'", lockName);
        } catch (Exception e) {
            closeConnectionReleasingAnyGrantedLock();
            logger.debug("failed to acquire advisory lock '{}'", lockName, e);
            throw (e instanceof FencepostException fe) ? fe : new FencepostException("Failed to acquire advisory lock: " + lockName, e);
        }
    }

    @Override
    public void lock(Duration timeout) {
        Durations.requireAtLeastOneMillisecond(timeout, "timeout");
        ensureNotHeld();
        boolean borrowedAutoCommit = true;
        try {
            connection = dataSource.getConnection();
            borrowedAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                Jdbc.setLockTimeout(connection, timeout);
                Jdbc.query(connection, sql.lock)
                  .bind(advisoryKey)
                  .map(ResultSet::next);
                connection.commit();
            } catch (SQLException e) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    e.addSuppressed(rollbackFailure);
                }
                if (SqlStates.LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                    logger.debug("timed out acquiring advisory lock '{}' after {}", lockName, timeout);
                    throw new LockAcquisitionTimeoutException(lockName);
                }
                throw e;
            } finally {
                restoreAutoCommit(borrowedAutoCommit);
            }
            held = true;
            logger.debug("acquired advisory lock '{}'", lockName);
        } catch (LockAcquisitionTimeoutException e) {
            closeConnectionReleasingAnyGrantedLock();
            throw e;
        } catch (Exception e) {
            closeConnectionReleasingAnyGrantedLock();
            logger.debug("failed to acquire advisory lock '{}'", lockName, e);
            throw (e instanceof FencepostException fe) ? fe : new FencepostException("Failed to acquire advisory lock: " + lockName, e);
        }
    }

    @Override
    public boolean tryLock() {
        ensureNotHeld();
        try {
            connection = dataSource.getConnection();
            boolean acquired = Jdbc.query(connection, sql.tryLock)
              .bind(advisoryKey)
              .map(rs -> rs.next() && rs.getBoolean(1));
            if (!acquired) {
                closeConnection();
                connection = null;
                logger.debug("tryLock failed for advisory lock '{}' - already held", lockName);
                return false;
            }
            commitIfInTransaction();
            held = true;
            logger.debug("acquired advisory lock '{}' via tryLock", lockName);
            return true;
        } catch (Exception e) {
            closeConnectionReleasingAnyGrantedLock();
            logger.debug("failed to tryLock advisory lock '{}'", lockName, e);
            throw (e instanceof FencepostException fe) ? fe : new FencepostException("Failed to try-lock advisory: " + lockName, e);
        }
    }

    @Override
    public void unlock() {
        if (!held) {
            throw new LockNotHeldException(lockName);
        }
        try {
            boolean released = Jdbc.query(connection, sql.unlock)
              .bind(advisoryKey)
              .map(rs -> {
                  rs.next();
                  return rs.getBoolean(1);
              });
            if (!released) {
                throw new LockNotHeldException(lockName);
            }
            logger.debug("released advisory lock '{}'", lockName);
        } catch (SQLException e) {
            throw new FencepostException("Failed to release advisory lock: " + lockName, e);
        } finally {
            try {
                Jdbc.execute(connection, sql.unlockAll);
            } catch (SQLException e) {
                logger.trace("failed to pg_advisory_unlock_all for '{}'", lockName, e);
            }
            closeConnection();
            connection = null;
            held = false;
        }
    }

    @Override
    public void close() {
        if (held) {
            try {
                unlock();
            } catch (Exception e) {
                logger.trace("failed to unlock advisory lock '{}' during close", lockName, e);
            }
        }
    }

    private void ensureNotHeld() {
        if (held) {
            throw new IllegalStateException("Lock already held: " + lockName);
        }
    }

    private void commitIfInTransaction() throws SQLException {
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
    }

    private void restoreAutoCommit(boolean autoCommit) {
        try {
            if (connection != null) {
                connection.setAutoCommit(autoCommit);
            }
        } catch (SQLException e) {
            logger.trace("failed to restore autoCommit for advisory lock '{}' connection", lockName, e);
        }
    }

    // On acquisition failure PostgreSQL may have still granted the lock server-side before we saw the error
    private void closeConnectionReleasingAnyGrantedLock() {
        if (connection == null) {
            return;
        }
        boolean released = false;
        try {
            Jdbc.execute(connection, sql.unlockAll);
            released = true;
        } catch (SQLException e) {
            logger.trace("failed to release advisory lock '{}' after acquisition failure", lockName, e);
        }
        if (released) {
            closeConnection();
        } else {
            discardConnection();
        }
        connection = null;
    }

    private void closeConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.trace("failed to close advisory lock '{}' connection", lockName, e);
        }
    }

    private void discardConnection() {
        try {
            connection.abort(Runnable::run);
        } catch (SQLException e) {
            logger.trace("failed to abort advisory lock '{}' connection", lockName, e);
        }
    }
}
