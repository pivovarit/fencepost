package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

final class AdvisoryLockInstance implements FencepostLock {

    private static final int ADVISORY_NAMESPACE = 2025588817; // "fencepost".hashCode();

    private final String lockName;
    private final DataSource dataSource;

    private volatile Connection connection;
    private volatile boolean held;

    AdvisoryLockInstance(String lockName, DataSource dataSource) {
        this.lockName = lockName;
        this.dataSource = dataSource;
    }

    @Override
    public void lock() {
        ensureNotHeld();
        try {
            connection = dataSource.getConnection();
            Jdbc.query(connection, "SELECT pg_advisory_lock(?, ?)")
              .bind(ADVISORY_NAMESPACE)
              .bind(advisoryKey())
              .map(ResultSet::next);
            held = true;
        } catch (Exception e) {
            closeConnection();
            connection = null;
            throw (e instanceof FencepostException) ? (FencepostException) e : new FencepostException("Failed to acquire advisory lock: " + lockName, e);
        }
    }

    @Override
    public void lock(Duration timeout) {
        ensureNotHeld();
        try {
            connection = dataSource.getConnection();
            Jdbc.setLockTimeout(connection, timeout);
            try {
                Jdbc.query(connection, "SELECT pg_advisory_lock(?, ?)")
                  .bind(ADVISORY_NAMESPACE)
                  .bind(advisoryKey())
                  .map(ResultSet::next);
            } catch (SQLException e) {
                if (SqlStates.LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                    throw new LockAcquisitionTimeoutException(lockName);
                }
                throw e;
            } finally {
                try {
                    Jdbc.resetLockTimeout(connection);
                } catch (SQLException ignored) {
                }
            }
            held = true;
        } catch (LockAcquisitionTimeoutException e) {
            closeConnection();
            connection = null;
            throw e;
        } catch (Exception e) {
            closeConnection();
            connection = null;
            throw (e instanceof FencepostException) ? (FencepostException) e : new FencepostException("Failed to acquire advisory lock: " + lockName, e);
        }
    }

    @Override
    public boolean tryLock() {
        ensureNotHeld();
        try {
            connection = dataSource.getConnection();
            boolean acquired = Jdbc.query(connection, "SELECT pg_try_advisory_lock(?, ?)")
              .bind(ADVISORY_NAMESPACE)
              .bind(advisoryKey())
              .map(rs -> rs.next() && rs.getBoolean(1));
            if (!acquired) {
                closeConnection();
                connection = null;
                return false;
            }
            held = true;
            return true;
        } catch (Exception e) {
            closeConnection();
            connection = null;
            throw (e instanceof FencepostException) ? (FencepostException) e : new FencepostException("Failed to try-lock advisory: " + lockName, e);
        }
    }

    @Override
    public void unlock() {
        if (!held) {
            throw new LockNotHeldException(lockName);
        }
        try {
            boolean released = Jdbc.query(connection, "SELECT pg_advisory_unlock(?, ?)")
              .bind(ADVISORY_NAMESPACE)
              .bind(advisoryKey())
              .map(rs -> {
                  rs.next();
                  return rs.getBoolean(1);
              });
            if (!released) {
                throw new LockNotHeldException(lockName);
            }
        } catch (SQLException e) {
            throw new FencepostException("Failed to release advisory lock: " + lockName, e);
        } finally {
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
            } catch (Exception ignored) {
            }
        }
    }

    private void ensureNotHeld() {
        if (held) {
            throw new IllegalStateException("Lock already held: " + lockName);
        }
    }

    private int advisoryKey() {
        return lockName.hashCode();
    }

    private void closeConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ignored) {
        }
    }
}
