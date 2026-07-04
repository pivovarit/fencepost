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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

final class FencepostQueue implements Queue {

    private static final Logger logger = LoggerFactory.getLogger(FencepostQueue.class);

    private static final String NOTIFY_DASHBOARD_SQL = "NOTIFY " + FencepostDashboard.DASHBOARD_CHANNEL;

    private final String queueName;
    private final DataSource dataSource;
    private final long visibilityTimeoutMs;
    private final long pollIntervalMs;
    private final int maxDeliveries;
    private final ListenerConnection listener;
    private final Sql sql;
    private final AckableMessage.Sql ackSql;

    private volatile boolean closed;
    private volatile boolean polling;

    FencepostQueue(String queueName, DataSource dataSource, String tableName,
                   Duration visibilityTimeout, long pollIntervalMs, int maxDeliveries) {
        this.queueName = queueName;
        this.dataSource = dataSource;
        this.visibilityTimeoutMs = visibilityTimeout.toMillis();
        this.pollIntervalMs = pollIntervalMs;
        this.maxDeliveries = maxDeliveries;
        var channelName = "fencepost_q_" + Long.toUnsignedString(HashUtils.fnv1a64("fencepost:" + queueName));
        this.listener = new ListenerConnection(dataSource, channelName);
        this.sql = new Sql(tableName, channelName, maxDeliveries);
        this.ackSql = new AckableMessage.Sql(tableName);
    }

    private static final class Sql {
        final String enqueue;
        final String notifyQueue;
        final String dequeue;

        Sql(String tableName, String channelName, int maxDeliveries) {
            this.enqueue = """
                INSERT INTO %s (queue_name, payload, type, headers, visible_at)
                VALUES (?, ?, ?, ?::jsonb, now() + %s)""".formatted(tableName, Jdbc.intervalMillis());
            this.notifyQueue = "NOTIFY " + channelName;
            this.dequeue = maxDeliveries > 0
              ? """
                WITH exhausted AS (
                    UPDATE %s SET dead_at = now(), visible_at = now(), picked_by = NULL,
                        last_error = 'exceeded max deliveries (' || attempts || ')'
                    WHERE id IN (SELECT id FROM %s WHERE queue_name = ? AND visible_at <= now() AND dead_at IS NULL AND attempts >= %d
                    FOR UPDATE SKIP LOCKED)
                )
                UPDATE %s SET visible_at = now() + %s, picked_by = ?, attempts = attempts + 1
                WHERE id = (SELECT id FROM %s WHERE queue_name = ? AND visible_at <= now() AND dead_at IS NULL AND attempts < %d
                ORDER BY visible_at, id LIMIT 1 FOR UPDATE SKIP LOCKED)
                RETURNING id, payload, type, headers, attempts""".formatted(tableName, tableName, maxDeliveries, tableName, Jdbc.intervalMillis(), tableName, maxDeliveries)
              : """
                UPDATE %s SET visible_at = now() + %s, picked_by = ?, attempts = attempts + 1
                WHERE id = (SELECT id FROM %s WHERE queue_name = ? AND visible_at <= now() AND dead_at IS NULL
                ORDER BY visible_at, id LIMIT 1 FOR UPDATE SKIP LOCKED)
                RETURNING id, payload, type, headers, attempts""".formatted(tableName, Jdbc.intervalMillis(), tableName);
        }
    }

    @Override
    public void enqueue(byte[] payload, String type, Map<String, String> headers) {
        enqueue(payload, type, headers, Duration.ZERO);
    }

    @Override
    public void enqueue(byte[] payload, String type, Map<String, String> headers, Duration delay) {
        requireOpen();
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
                logger.debug("enqueued message to queue '{}'", queueName);
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                try {
                    conn.setAutoCommit(borrowedAutoCommit);
                } catch (SQLException e) {
                    logger.trace("failed to restore autoCommit after enqueueing to queue '{}'", queueName, e);
                }
            }
        } catch (SQLException e) {
            throw new FencepostException("Failed to enqueue message to queue: " + queueName, e);
        }
    }

    @Override
    public Optional<Message> tryDequeue() {
        requireOpen();
        String pickToken = TableBasedLock.HOSTNAME + "/" + Thread.currentThread().getName() + "/" + Long.toHexString(ThreadLocalRandom.current().nextLong()) + Long.toHexString(ThreadLocalRandom.current().nextLong());

        try {
            Jdbc.Query query = Jdbc.query(dataSource, sql.dequeue);
            if (maxDeliveries > 0) {
                query.bind(queueName);
            }
            return query
              .bind(visibilityTimeoutMs)
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
                    pickToken, dataSource, ackSql));
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
        Durations.requireAtLeastOneMillisecond(timeout, "timeout");
        return dequeueBlocking(timeout);
    }

    private Message dequeueBlocking(Duration timeout) {
        requireOpen();
        long deadlineNanos = timeout != null
          ? System.nanoTime() + timeout.toNanos()
          : Long.MAX_VALUE;

        while (true) {
            Optional<Message> result = tryDequeue();
            if (result.isPresent()) {
                return result.get();
            }

            long waitMs = pollIntervalMs;
            if (timeout != null) {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
                if (remainingMs <= 0) {
                    throw new FencepostException("Dequeue timed out on queue: " + queueName);
                }
                waitMs = Math.min(waitMs, remainingMs);
            }
            try {
                waitForNotification(listener.ensure(), waitMs);
            } catch (SQLException e) {
                throw new FencepostException("Failed to set up listener for queue: " + queueName, e);
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        listener.stop();
        synchronized (listener.lock()) {
            listener.lock().notifyAll();
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("Queue '" + queueName + "' is closed");
        }
    }

    private void waitForNotification(Connection conn, long waitMs) {
        synchronized (listener.lock()) {
            if (polling) {
                try {
                    listener.lock().wait(waitMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return;
            }
            polling = true;
        }
        try {
            var pgConn = conn.unwrap(PGConnection.class);
            pgConn.getNotifications((int) Math.min(waitMs, Integer.MAX_VALUE));
        } catch (Exception e) {
            listener.close();
            if (!listener.isStopped()) {
                synchronized (listener.lock()) {
                    if (!listener.isStopped()) {
                        try {
                            listener.lock().wait(waitMs);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        } finally {
            synchronized (listener.lock()) {
                polling = false;
                listener.lock().notifyAll();
            }
        }
    }

}
