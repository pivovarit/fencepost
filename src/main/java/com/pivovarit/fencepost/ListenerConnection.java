package com.pivovarit.fencepost;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

final class ListenerConnection {

    private static final Logger logger = LoggerFactory.getLogger(ListenerConnection.class);

    private final DataSource dataSource;
    private final String channelName;
    private final Object lock = new Object();
    private volatile Connection connection;
    private volatile boolean stopped;

    ListenerConnection(DataSource dataSource, String channelName) {
        this.dataSource = dataSource;
        this.channelName = channelName;
    }

    Connection ensure() throws SQLException {
        synchronized (lock) {
            if (stopped) {
                throw new SQLException("Listener is stopped");
            }
            if (connection != null) {
                try {
                    if (!connection.isClosed()) {
                        return connection;
                    }
                } catch (SQLException e) {
                    logger.trace("failed to check listener connection state", e);
                    try {
                        connection.close();
                    } catch (SQLException ce) {
                        logger.trace("failed to close listener connection", ce);
                    }
                    connection = null;
                }
            }
        }

        Connection newConn = null;
        try {
            newConn = dataSource.getConnection();
            newConn.setAutoCommit(true);
            Jdbc.execute(newConn, "LISTEN " + channelName);
        } catch (SQLException e) {
            if (newConn != null) {
                try {
                    newConn.close();
                } catch (SQLException ce) {
                    logger.trace("failed to close listener connection", ce);
                }
            }
            throw e;
        }

        synchronized (lock) {
            if (stopped) {
                try {
                    newConn.close();
                } catch (SQLException e) {
                    logger.trace("failed to close listener connection", e);
                }
                throw new SQLException("Listener is stopped");
            }
            if (connection != null) {
                try {
                    if (!connection.isClosed()) {
                        Connection winner = connection;
                        try {
                            newConn.close();
                        } catch (SQLException e) {
                            logger.trace("failed to close listener connection", e);
                        }
                        return winner;
                    }
                } catch (SQLException e) {
                    logger.trace("failed to check listener connection state", e);
                    try {
                        connection.close();
                    } catch (SQLException ce) {
                        logger.trace("failed to close listener connection", ce);
                    }
                    connection = null;
                }
            }
            connection = newConn;
            return newConn;
        }
    }

    void close() {
        Connection toClose;
        synchronized (lock) {
            toClose = connection;
            connection = null;
        }
        if (toClose == null) {
            return;
        }
        try {
            if (!toClose.isClosed()) {
                Jdbc.execute(toClose, "UNLISTEN *");
            }
        } catch (SQLException e) {
            logger.trace("failed to UNLISTEN before releasing listener connection", e);
        }
        try {
            toClose.close();
        } catch (SQLException e) {
            logger.trace("failed to close listener connection", e);
        }
    }

    void abort() {
        Connection conn = connection;
        if (conn != null) {
            try {
                conn.abort(Runnable::run);
            } catch (SQLException e) {
                logger.trace("failed to abort listener connection", e);
            }
        }
    }

    void stop() {
        stopped = true;
        abort();
        close();
    }

    Object lock() {
        return lock;
    }

    boolean isStopped() {
        return stopped;
    }
}
