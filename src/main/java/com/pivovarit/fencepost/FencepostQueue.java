package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.Message;
import com.pivovarit.fencepost.queue.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

final class FencepostQueue implements Queue {

    private static final Logger logger = LoggerFactory.getLogger(FencepostQueue.class);

    private final String queueName;
    private final DataSource dataSource;
    private final String tableName;
    private final Duration visibilityTimeout;
    private final long pollIntervalMs;

    private volatile Connection listenerConnection;

    FencepostQueue(String queueName, DataSource dataSource, String tableName,
                   Duration visibilityTimeout, long pollIntervalMs) {
        this.queueName = queueName;
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.visibilityTimeout = visibilityTimeout;
        this.pollIntervalMs = pollIntervalMs;
    }

    @Override
    public void enqueue(byte[] payload) {
        enqueue(payload, null, null, Duration.ZERO);
    }

    @Override
    public void enqueue(byte[] payload, Duration delay) {
        enqueue(payload, null, null, delay);
    }

    @Override
    public void enqueue(byte[] payload, String type, Map<String, String> headers) {
        enqueue(payload, type, headers, Duration.ZERO);
    }

    @Override
    public void enqueue(byte[] payload, String type, Map<String, String> headers, Duration delay) {
        if (delay.isNegative()) {
            throw new IllegalArgumentException("delay must not be negative");
        }
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                Jdbc.update(conn, String.format(
                    "INSERT INTO %s (queue_name, payload, type, headers, visible_at) VALUES (?, ?, ?, ?::jsonb, now() + %s)",
                    tableName, Jdbc.intervalMillis()))
                  .bind(queueName)
                  .bind(payload)
                  .bind(type)
                  .bind(HeadersCodec.toJson(headers))
                  .bind(delay.toMillis())
                  .execute();
                Jdbc.execute(conn, "NOTIFY " + channelName());
                conn.commit();
                logger.debug("enqueued message to queue '{}'", queueName);
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new FencepostException("Failed to enqueue message to queue: " + queueName, e);
        }
    }

    @Override
    public Optional<Message> tryDequeue() {
        String pickToken = TableBasedLock.HOSTNAME + "/" + Thread.currentThread().getName() + "/" + UUID.randomUUID();
        String sql = String.format(
            "UPDATE %s SET visible_at = now() + %s, picked_by = ?, attempts = attempts + 1 "
              + "WHERE id = (SELECT id FROM %s WHERE queue_name = ? AND visible_at <= now() "
              + "ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING id, payload, type, headers, attempts",
            tableName, Jdbc.intervalMillis(), tableName);

        try {
            return Jdbc.query(dataSource, sql)
              .bind(visibilityTimeout.toMillis())
              .bind(pickToken)
              .bind(queueName)
              .map(rs -> {
                  if (!rs.next()) {
                      return Optional.empty();
                  }
                  long id = rs.getLong(1);
                  logger.debug("dequeued message id={} from queue '{}'", id, queueName);
                  return Optional.of(new AckableMessage(
                    id, rs.getBytes(2), rs.getString(3), HeadersCodec.fromJson(rs.getString(4)), rs.getInt(5),
                    pickToken, dataSource, tableName));
              });
        } catch (SQLException e) {
            throw new FencepostException("Failed to dequeue from queue: " + queueName, e);
        }
    }

    @Override
    public Message dequeue() {
        return dequeueBlocking(null);
    }

    @Override
    public Message dequeue(Duration timeout) {
        return dequeueBlocking(timeout);
    }

    private Message dequeueBlocking(Duration timeout) {
        long deadlineNanos = timeout != null
          ? System.nanoTime() + timeout.toNanos()
          : Long.MAX_VALUE;

        while (true) {
            Optional<Message> result = tryDequeue();
            if (result.isPresent()) {
                return result.get();
            }

            if (timeout != null && System.nanoTime() >= deadlineNanos) {
                throw new FencepostException("Dequeue timed out on queue: " + queueName);
            }

            waitForNotification(ensureListening());
        }
    }

    @Override
    public void close() {
        closeListenerConnection();
    }

    private String channelName() {
        return "fencepost_q_" + Long.toUnsignedString(HashUtils.fnv1a64("fencepost:" + queueName));
    }


    private synchronized Connection ensureListening() {
        if (listenerConnection != null) {
            try {
                if (!listenerConnection.isClosed()) {
                    return listenerConnection;
                }
            } catch (SQLException e) {
                logger.trace("failed to check listener connection state", e);
            }
        }
        try {
            listenerConnection = dataSource.getConnection();
            listenerConnection.setAutoCommit(true);
            Jdbc.execute(listenerConnection, "LISTEN " + channelName());
            return listenerConnection;
        } catch (SQLException e) {
            closeListenerConnection();
            throw new FencepostException("Failed to set up listener for queue: " + queueName, e);
        }
    }

    private void waitForNotification(Connection conn) {
        try {
            var pgConn = conn.unwrap(PGConnection.class);
            pgConn.getNotifications((int) pollIntervalMs);
        } catch (Exception e) {
            closeListenerConnection();
            try {
                Thread.sleep(pollIntervalMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new FencepostException("Interrupted while waiting for messages on queue: " + queueName);
            }
        }
    }

    private synchronized void closeListenerConnection() {
        if (listenerConnection != null) {
            try {
                listenerConnection.close();
            } catch (SQLException e) {
                logger.trace("failed to close listener connection", e);
            }
            listenerConnection = null;
        }
    }

}
