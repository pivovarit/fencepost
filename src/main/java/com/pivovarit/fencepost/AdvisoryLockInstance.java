package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

final class AdvisoryLockInstance implements FencepostLock {

    private static final int ADVISORY_NAMESPACE = "fencepost".hashCode();

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
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT pg_advisory_lock(?, ?)")) {
                ps.setInt(1, ADVISORY_NAMESPACE);
                ps.setInt(2, advisoryKey());
                ps.execute();
            }
            held = true;
        } catch (Exception e) {
            closeConnection();
            connection = null;
            throw (e instanceof FencepostException) ? (FencepostException) e
                : new FencepostException("Failed to acquire advisory lock: " + lockName, e);
        }
    }

    @Override
    public void lock(Duration timeout) {
        ensureNotHeld();
        try {
            connection = dataSource.getConnection();
            try (PreparedStatement set = connection.prepareStatement(
                    "SET lock_timeout = '" + timeout.toMillis() + "ms'")) {
                set.execute();
            }
            try {
                try (PreparedStatement ps = connection.prepareStatement(
                        "SELECT pg_advisory_lock(?, ?)")) {
                    ps.setInt(1, ADVISORY_NAMESPACE);
                    ps.setInt(2, advisoryKey());
                    ps.execute();
                }
            } catch (SQLException e) {
                if (SqlStates.LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                    throw new LockAcquisitionTimeoutException(lockName);
                }
                throw e;
            } finally {
                try (PreparedStatement reset = connection.prepareStatement(
                        "SET lock_timeout = 0")) {
                    reset.execute();
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
            throw (e instanceof FencepostException) ? (FencepostException) e
                : new FencepostException("Failed to acquire advisory lock: " + lockName, e);
        }
    }

    @Override
    public boolean tryLock() {
        ensureNotHeld();
        try {
            connection = dataSource.getConnection();
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT pg_try_advisory_lock(?, ?)")) {
                ps.setInt(1, ADVISORY_NAMESPACE);
                ps.setInt(2, advisoryKey());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next() || !rs.getBoolean(1)) {
                        closeConnection();
                        connection = null;
                        return false;
                    }
                }
            }
            held = true;
            return true;
        } catch (Exception e) {
            closeConnection();
            connection = null;
            throw (e instanceof FencepostException) ? (FencepostException) e
                : new FencepostException("Failed to try-lock advisory: " + lockName, e);
        }
    }

    @Override
    public void unlock() {
        if (!held) {
            throw new LockNotHeldException(lockName);
        }
        try {
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT pg_advisory_unlock(?, ?)")) {
                ps.setInt(1, ADVISORY_NAMESPACE);
                ps.setInt(2, advisoryKey());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    if (!rs.getBoolean(1)) {
                        throw new LockNotHeldException(lockName);
                    }
                }
            }
        } catch (LockNotHeldException e) {
            throw e;
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
