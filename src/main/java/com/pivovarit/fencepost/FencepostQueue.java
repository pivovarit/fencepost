package com.pivovarit.fencepost;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

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
    public void enqueue(String payload) {
        enqueue(payload, null, null, Duration.ZERO);
    }

    @Override
    public void enqueue(String payload, Duration delay) {
        enqueue(payload, null, null, delay);
    }

    @Override
    public void enqueue(String payload, String type, Map<String, String> headers) {
        enqueue(payload, type, headers, Duration.ZERO);
    }

    @Override
    public void enqueue(String payload, String type, Map<String, String> headers, Duration delay) {
        if (delay.isNegative()) {
            throw new IllegalArgumentException("delay must not be negative");
        }
        try {
            Jdbc.update(dataSource, String.format(
                "INSERT INTO %s (queue_name, payload, type, headers, visible_at) VALUES (?, ?, ?, ?::jsonb, now() + %s)",
                tableName, Jdbc.intervalMillis()))
              .bind(queueName)
              .bind(payload)
              .bind(type)
              .bind(HeadersCodec.toJson(headers))
              .bind(delay.toMillis())
              .execute();
            logger.debug("enqueued message to queue '{}'", queueName);
            notify_();
        } catch (SQLException e) {
            throw new FencepostException("Failed to enqueue message to queue: " + queueName, e);
        }
    }

    @Override
    public Optional<Message> tryDequeue() {
        String pickedBy = TableBasedLock.HOSTNAME + "/" + Thread.currentThread().getName();
        String sql = String.format(
            "UPDATE %s SET visible_at = now() + %s, picked_by = ?, attempts = attempts + 1 "
              + "WHERE id = (SELECT id FROM %s WHERE queue_name = ? AND visible_at <= now() "
              + "ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING id, payload, type, headers, attempts",
            tableName, Jdbc.intervalMillis(), tableName);

        try {
            return Jdbc.query(dataSource, sql)
              .bind(visibilityTimeout.toMillis())
              .bind(pickedBy)
              .bind(queueName)
              .map(rs -> {
                  if (!rs.next()) {
                      return Optional.<Message>empty();
                  }
                  long id = rs.getLong(1);
                  logger.debug("dequeued message id={} from queue '{}'", id, queueName);
                  return Optional.<Message>of(new AckableMessage(
                    id, rs.getString(2), rs.getString(3), HeadersCodec.fromJson(rs.getString(4)), rs.getInt(5),
                    dataSource, tableName));
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

        ensureListening();

        while (true) {
            Optional<Message> result = tryDequeue();
            if (result.isPresent()) {
                return result.get();
            }

            if (timeout != null && System.nanoTime() >= deadlineNanos) {
                throw new FencepostException("Dequeue timed out on queue: " + queueName);
            }

            waitForNotification();
        }
    }

    @Override
    public void close() {
        closeListenerConnection();
    }

    private String channelName() {
        return "fencepost_q_" + HashUtils.fnv1a64("fencepost:" + queueName);
    }

    private void notify_() {
        try (Connection conn = dataSource.getConnection()) {
            Jdbc.execute(conn, "NOTIFY " + channelName());
        } catch (SQLException e) {
            // notification is best-effort; polling is the fallback
        }
    }

    private synchronized void ensureListening() {
        if (listenerConnection != null) {
            try {
                if (!listenerConnection.isClosed()) {
                    return;
                }
            } catch (SQLException ignored) {
            }
        }
        try {
            listenerConnection = dataSource.getConnection();
            listenerConnection.setAutoCommit(true);
            Jdbc.execute(listenerConnection, "LISTEN " + channelName());
        } catch (SQLException e) {
            closeListenerConnection();
            throw new FencepostException("Failed to set up listener for queue: " + queueName, e);
        }
    }

    private void waitForNotification() {
        try {
            var pgConn = listenerConnection.unwrap(Class.forName("org.postgresql.PGConnection"));
            var method = pgConn.getClass().getMethod("getNotifications", int.class);
            method.invoke(pgConn, (int) pollIntervalMs);
        } catch (Exception e) {
            if (e instanceof java.lang.reflect.InvocationTargetException) {
                Throwable cause = e.getCause();
                if (cause instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    throw new FencepostException("Interrupted while waiting for messages on queue: " + queueName);
                }
            }
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
            } catch (SQLException ignored) {
            }
            listenerConnection = null;
        }
    }

}
