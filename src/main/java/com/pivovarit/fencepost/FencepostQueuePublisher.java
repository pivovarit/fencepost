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
    private final String enqueueSql;
    private final String notifyQueueSql;

    FencepostQueuePublisher(String queueName, DataSource dataSource, String tableName) {
        this.queueName = queueName;
        this.dataSource = dataSource;
        this.enqueueSql = String.format(
            "INSERT INTO %s (queue_name, payload, type, headers, visible_at) VALUES (?, ?, ?, ?::jsonb, now() + %s)",
            tableName, Jdbc.intervalMillis());
        var channelName = "fencepost_q_" + Long.toUnsignedString(HashUtils.fnv1a64("fencepost:" + queueName));
        this.notifyQueueSql = "NOTIFY " + channelName;
    }

    @Override
    public void publish(byte[] payload, String type, Map<String, String> headers, Duration delay) {
        Objects.requireNonNull(type, "type must not be null");
        HeadersCodec.requirePrintable(type, "Message type");
        long delayMillis = Durations.toNonNegativeMillis(delay, "delay");
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                Jdbc.update(conn, enqueueSql)
                    .bind(queueName)
                    .bind(payload)
                    .bind(type)
                    .bind(HeadersCodec.toJson(headers))
                    .bind(delayMillis)
                    .execute();
                Jdbc.execute(conn, notifyQueueSql);
                Jdbc.execute(conn, NOTIFY_DASHBOARD_SQL);
                conn.commit();
                logger.debug("published message to queue '{}'", queueName);
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new FencepostException("Failed to publish message to queue: " + queueName, e);
        }
    }

    @Override
    public TransactionalQueuePublisher transactional(Connection connection) {
        Objects.requireNonNull(connection, "connection must not be null");
        return new FencepostTransactionalQueuePublisher(queueName, connection, enqueueSql, notifyQueueSql);
    }
}
