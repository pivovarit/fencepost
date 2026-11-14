package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.DeadLetters;
import com.pivovarit.fencepost.queue.DeadMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class FencepostDeadLetters implements DeadLetters {

    private static final Logger logger = LoggerFactory.getLogger(FencepostDeadLetters.class);
    private static final String NOTIFY_DASHBOARD_SQL = "NOTIFY " + FencepostDashboard.DASHBOARD_CHANNEL;

    private final String queueName;
    private final DataSource dataSource;
    private final Sql sql;

    FencepostDeadLetters(String queueName, DataSource dataSource, String tableName) {
        this.queueName = queueName;
        this.dataSource = dataSource;
        this.sql = new Sql(tableName, QueueChannels.name(queueName));
    }

    private static final class Sql {
        final String list;
        final String count;
        final String redrive;
        final String redriveAll;
        final String purge;
        final String purgeAll;
        final String notifyQueue;

        Sql(String tableName, String channelName) {
            this.list = """
                SELECT id, payload, type, headers, attempts, last_error, dead_at
                FROM %s WHERE queue_name = ? AND dead_at IS NOT NULL
                ORDER BY dead_at, id LIMIT ?""".formatted(tableName);
            this.count = "SELECT count(*) FROM %s WHERE queue_name = ? AND dead_at IS NOT NULL".formatted(tableName);
            this.redrive = """
                UPDATE %s SET dead_at = NULL, last_error = NULL, attempts = 0, visible_at = now(), picked_by = NULL
                WHERE id = ? AND queue_name = ? AND dead_at IS NOT NULL""".formatted(tableName);
            this.redriveAll = """
                UPDATE %s SET dead_at = NULL, last_error = NULL, attempts = 0, visible_at = now(), picked_by = NULL
                WHERE id IN (SELECT id FROM %s WHERE queue_name = ? AND dead_at IS NOT NULL
                ORDER BY dead_at, id LIMIT ? FOR UPDATE SKIP LOCKED)""".formatted(tableName, tableName);
            this.purge = "DELETE FROM %s WHERE id = ? AND queue_name = ? AND dead_at IS NOT NULL".formatted(tableName);
            this.purgeAll = """
                DELETE FROM %s WHERE id IN (SELECT id FROM %s WHERE queue_name = ? AND dead_at IS NOT NULL
                ORDER BY dead_at, id LIMIT ? FOR UPDATE SKIP LOCKED)""".formatted(tableName, tableName);
            this.notifyQueue = "NOTIFY " + channelName;
        }
    }

    @Override
    public List<DeadMessage> list(int limit) {
        requireAtLeastOne(limit, "limit");
        try {
            return Jdbc.query(dataSource, sql.list)
              .bind(queueName)
              .bind(limit)
              .map(rs -> {
                  List<DeadMessage> result = new ArrayList<>();
                  while (rs.next()) {
                      result.add(new DeadMessage(
                        rs.getLong(1), rs.getBytes(2), Optional.ofNullable(rs.getString(3)),
                        HeadersCodec.fromJson(rs.getString(4)), rs.getInt(5),
                        Optional.ofNullable(rs.getString(6)), rs.getTimestamp(7).toInstant()));
                  }
                  return List.copyOf(result);
              });
        } catch (SQLException e) {
            throw new FencepostException("Failed to list dead letters of queue: " + queueName, e);
        }
    }

    @Override
    public long count() {
        try {
            return Jdbc.query(dataSource, sql.count)
              .bind(queueName)
              .map(rs -> {
                  rs.next();
                  return rs.getLong(1);
              });
        } catch (SQLException e) {
            throw new FencepostException("Failed to count dead letters of queue: " + queueName, e);
        }
    }

    @Override
    public boolean redrive(long id) {
        int redriven = updateAndNotify("redrive dead letter " + id, sql.redrive, id, queueName);
        return redriven > 0;
    }

    @Override
    public int redriveAll(int max) {
        requireAtLeastOne(max, "max");
        return updateAndNotify("redrive dead letters", sql.redriveAll, queueName, max);
    }

    /** Runs the update and, if it changed rows, wakes queue consumers and the dashboard in the same transaction. */
    private int updateAndNotify(String action, String statement, Object... binds) {
        try (Connection conn = dataSource.getConnection()) {
            boolean borrowedAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                Jdbc.Update update = Jdbc.update(conn, statement);
                for (Object bind : binds) {
                    update.bind(bind);
                }
                int changed = update.execute();
                if (changed > 0) {
                    Jdbc.execute(conn, sql.notifyQueue);
                    Jdbc.execute(conn, NOTIFY_DASHBOARD_SQL);
                }
                conn.commit();
                logger.debug("{} in queue '{}': {} row(s)", action, queueName, changed);
                return changed;
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                try {
                    conn.setAutoCommit(borrowedAutoCommit);
                } catch (SQLException e) {
                    logger.trace("failed to restore autoCommit after '{}' in queue '{}'", action, queueName, e);
                }
            }
        } catch (SQLException e) {
            throw new FencepostException("Failed to " + action + " in queue: " + queueName, e);
        }
    }

    @Override
    public boolean purge(long id) {
        return delete("purge dead letter " + id, sql.purge, id, queueName) > 0;
    }

    @Override
    public int purgeAll(int max) {
        requireAtLeastOne(max, "max");
        return delete("purge dead letters", sql.purgeAll, queueName, max);
    }

    private int delete(String action, String statement, Object... binds) {
        try {
            Jdbc.Update update = Jdbc.update(dataSource, statement);
            for (Object bind : binds) {
                update.bind(bind);
            }
            int deleted = update.execute();
            logger.debug("{} in queue '{}': {} row(s)", action, queueName, deleted);
            return deleted;
        } catch (SQLException e) {
            throw new FencepostException("Failed to " + action + " in queue: " + queueName, e);
        }
    }

    private static void requireAtLeastOne(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be at least 1: " + value);
        }
    }
}
