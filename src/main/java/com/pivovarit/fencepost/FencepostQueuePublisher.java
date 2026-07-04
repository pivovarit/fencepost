package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.QueuePublisher;
import com.pivovarit.fencepost.queue.TransactionalQueuePublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

final class FencepostQueuePublisher implements QueuePublisher {

    private static final Logger logger = LoggerFactory.getLogger(FencepostQueuePublisher.class);
    private static final String NOTIFY_DASHBOARD_SQL = "NOTIFY " + FencepostDashboard.DASHBOARD_CHANNEL;

    private final String queueName;
    private final DataSource dataSource;
    private final Sql sql;

    FencepostQueuePublisher(String queueName, DataSource dataSource, String tableName) {
        this.queueName = queueName;
        this.dataSource = dataSource;
        var channelName = "fencepost_q_" + Long.toUnsignedString(HashUtils.fnv1a64("fencepost:" + queueName));
        this.sql = new Sql(tableName, channelName);
    }

    static final class Sql {
        final String enqueue;
        final String notifyQueue;

        Sql(String tableName, String channelName) {
            this.enqueue = """
                INSERT INTO %s (queue_name, payload, type, headers, visible_at)
                VALUES (?, ?, ?, ?::jsonb, now() + %s)""".formatted(tableName, Jdbc.intervalMillis());
            this.notifyQueue = "NOTIFY " + channelName;
        }
    }

    @Override
    public void publish(byte[] payload, String type, Map<String, String> headers, Duration delay) {
        Objects.requireNonNull(type, "type must not be null");
        HeadersCodec.requirePrintable(type, "Message type");
        long delayMillis = Durations.toNonNegativeMillis(delay, "delay");
        try (Connection conn = dataSource.getConnection()) {
            boolean borrowedAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                Jdbc.update(conn, sql.enqueue)
                    .bind(queueName)
                    .bind(payload)
                    .bind(type)
                    .bind(HeadersCodec.toJson(headers))
                    .bind(delayMillis)
                    .execute();
                Jdbc.execute(conn, sql.notifyQueue);
                Jdbc.execute(conn, NOTIFY_DASHBOARD_SQL);
                conn.commit();
                logger.debug("published message to queue '{}'", queueName);
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                try {
                    conn.setAutoCommit(borrowedAutoCommit);
                } catch (SQLException e) {
                    logger.trace("failed to restore autoCommit after publishing to queue '{}'", queueName, e);
                }
            }
        } catch (SQLException e) {
            throw new FencepostException("Failed to publish message to queue: " + queueName, e);
        }
    }

    @Override
    public TransactionalQueuePublisher transactional(Connection connection) {
        Objects.requireNonNull(connection, "connection must not be null");
        return new FencepostTransactionalQueuePublisher(queueName, connection, sql);
    }
}
