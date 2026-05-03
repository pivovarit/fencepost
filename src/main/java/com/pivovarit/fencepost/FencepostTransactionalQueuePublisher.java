package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.TransactionalQueuePublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

final class FencepostTransactionalQueuePublisher implements TransactionalQueuePublisher {

    private static final Logger logger = LoggerFactory.getLogger(FencepostTransactionalQueuePublisher.class);
    private static final String NOTIFY_DASHBOARD_SQL = "NOTIFY " + FencepostDashboard.DASHBOARD_CHANNEL;

    private final String queueName;
    private final Connection connection;
    private final String enqueueSql;
    private final String notifyQueueSql;

    FencepostTransactionalQueuePublisher(String queueName, Connection connection, String enqueueSql, String notifyQueueSql) {
        this.queueName = queueName;
        this.connection = connection;
        this.enqueueSql = enqueueSql;
        this.notifyQueueSql = notifyQueueSql;
    }

    @Override
    public void publish(byte[] payload, String type, Map<String, String> headers, Duration delay) {
        Objects.requireNonNull(type, "type must not be null");
        HeadersCodec.requirePrintable(type, "Message type");
        long delayMillis = Durations.toNonNegativeMillis(delay, "delay");
        try {
            Jdbc.update(connection, enqueueSql)
                .bind(queueName)
                .bind(payload)
                .bind(type)
                .bind(HeadersCodec.toJson(headers))
                .bind(delayMillis)
                .execute();
            Jdbc.execute(connection, notifyQueueSql);
            Jdbc.execute(connection, NOTIFY_DASHBOARD_SQL);
            logger.debug("published message to queue '{}' (transactional)", queueName);
        } catch (SQLException e) {
            throw new FencepostException("Failed to publish message to queue: " + queueName, e);
        }
    }
}
