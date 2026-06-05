package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public final class DashboardApi {

    private final DataSource dataSource;
    private final Queries queries;

    public DashboardApi(DataSource dataSource, String locksTable, String queueTable) {
        this.dataSource = dataSource;
        this.queries = new Queries(locksTable, queueTable);
    }

    public String status() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            boolean locksEnabled = tableExists(conn, queries.locksTable);
            boolean queuesEnabled = tableExists(conn, queries.queueTable);
            return "{\"locks_enabled\":" + locksEnabled + ",\"queues_enabled\":" + queuesEnabled + "}";
        }
    }

    public String locks() throws SQLException {
        return Jdbc.query(dataSource, queries.allLocks).map(rs -> {            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            while (rs.next()) {
                if (!first) {
                    sb.append(",");
                }
                first = false;
                appendLockRow(sb, rs);
            }
            sb.append("]");
            return sb.toString();
        });
    }

    public String lock(String name) throws SQLException {
        return Jdbc.query(dataSource, queries.lockByName).bind(name).map(rs -> {
            if (!rs.next()) {
                return "null";
            }
            StringBuilder sb = new StringBuilder();
            appendLockRow(sb, rs);
            return sb.toString();
        });
    }

    public String queues() throws SQLException {
        return Jdbc.query(dataSource, queries.allQueues).map(rs -> {
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            while (rs.next()) {
                if (!first) {
                    sb.append(",");
                }
                first = false;
                appendQueueSummaryRow(sb, rs);
            }
            sb.append("]");
            return sb.toString();
        });
    }

    public String queue(String name) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            String summaryJson = Jdbc.query(conn, queries.queueByName).bind(name).map(rs -> {
                if (!rs.next()) {
                    return "{\"name\":" + jsonString(name) +
                           ",\"total\":0,\"visible\":0,\"in_flight\":0,\"oldest_age_seconds\":null";
                }
                StringBuilder sb = new StringBuilder();
                appendQueueSummaryFields(sb, rs);
                return sb.toString();
            });

            String messagesJson = Jdbc.query(conn, queries.messagesByQueue).bind(name).map(rs -> {
                StringBuilder sb = new StringBuilder("[");
                boolean first = true;
                while (rs.next()) {
                    if (!first) {
                        sb.append(",");
                    }
                    first = false;
                    appendMessageRow(sb, rs);
                }
                sb.append("]");
                return sb.toString();
            });

            return summaryJson + ",\"messages\":" + messagesJson + "}";
        }
    }

    public String message(String queueName, long id) throws SQLException {
        return Jdbc.query(dataSource, queries.messageById).bind(queueName).bind(id).map(rs -> {
            if (!rs.next()) {
                return "null";
            }
            StringBuilder sb = new StringBuilder("{");
            sb.append("\"id\":").append(rs.getLong("id")).append(",");
            sb.append("\"payload\":").append(jsonString(rs.getString("payload_b64"))).append(",");
            sb.append("\"type\":").append(jsonString(rs.getString("type"))).append(",");
            String headers = rs.getString("headers");
            sb.append("\"headers\":").append(headers != null ? headers : "null").append(",");
            sb.append("\"picked_by\":").append(jsonString(rs.getString("picked_by"))).append(",");
            sb.append("\"attempts\":").append(rs.getInt("attempts")).append(",");
            Object visibleAt = rs.getObject("visible_at");
            sb.append("\"visible_at\":").append(visibleAt == null ? "null" : jsonString(visibleAt.toString()))
              .append(",");
            Object createdAt = rs.getObject("created_at");
            sb.append("\"created_at\":").append(createdAt == null ? "null" : jsonString(createdAt.toString()))
              .append(",");
            sb.append("\"status\":").append(jsonString(rs.getString("status")));
            sb.append("}");
            return sb.toString();
        });
    }

    private static boolean tableExists(Connection conn, String tableName) throws SQLException {
        String[] parts = tableName.split("\\.", 2);
        if (parts.length == 2) {
            String schema = parts[0];
            String table = parts[1];
            return Jdbc.query(conn, Queries.TABLE_EXISTS_IN_SCHEMA).bind(schema).bind(table).map(rs -> {
                rs.next();
                return rs.getBoolean(1);
            });
        } else {
            return Jdbc.query(conn, Queries.TABLE_EXISTS).bind(tableName).map(rs -> {
                rs.next();
                return rs.getBoolean(1);
            });
        }
    }

    private static void appendLockRow(StringBuilder sb, java.sql.ResultSet rs) throws SQLException {
        sb.append("{");
        sb.append("\"name\":").append(jsonString(rs.getString("lock_name"))).append(",");
        sb.append("\"token\":").append(rs.getLong("token")).append(",");
        sb.append("\"locked_by\":").append(jsonString(rs.getString("locked_by"))).append(",");
        Object lockedAt = rs.getObject("locked_at");
        sb.append("\"locked_at\":").append(lockedAt == null ? "null" : jsonString(lockedAt.toString())).append(",");
        Object expiresAt = rs.getObject("expires_at");
        sb.append("\"expires_at\":").append(expiresAt == null ? "null" : jsonString(expiresAt.toString())).append(",");
        sb.append("\"status\":").append(jsonString(rs.getString("status")));
        sb.append("}");
    }

    private static void appendQueueSummaryRow(StringBuilder sb, java.sql.ResultSet rs) throws SQLException {
        appendQueueSummaryFields(sb, rs);
        sb.append("}");
    }

    private static void appendQueueSummaryFields(StringBuilder sb, java.sql.ResultSet rs) throws SQLException {
        sb.append("{");
        sb.append("\"name\":").append(jsonString(rs.getString("queue_name"))).append(",");
        sb.append("\"total\":").append(rs.getLong("total")).append(",");
        sb.append("\"visible\":").append(rs.getLong("visible")).append(",");
        sb.append("\"in_flight\":").append(rs.getLong("in_flight")).append(",");
        Object age = rs.getObject("oldest_age_seconds");
        sb.append("\"oldest_age_seconds\":").append(age == null ? "null" : age.toString());
    }

    private static void appendMessageRow(StringBuilder sb, java.sql.ResultSet rs) throws SQLException {
        sb.append("{");
        sb.append("\"id\":").append(rs.getLong("id")).append(",");
        sb.append("\"payload_preview\":").append(jsonString(rs.getString("payload_preview"))).append(",");
        sb.append("\"type\":").append(jsonString(rs.getString("type"))).append(",");
        sb.append("\"picked_by\":").append(jsonString(rs.getString("picked_by"))).append(",");
        sb.append("\"attempts\":").append(rs.getInt("attempts")).append(",");
        Object visibleAt = rs.getObject("visible_at");
        sb.append("\"visible_at\":").append(visibleAt == null ? "null" : jsonString(visibleAt.toString())).append(",");
        Object createdAt = rs.getObject("created_at");
        sb.append("\"created_at\":").append(createdAt == null ? "null" : jsonString(createdAt.toString())).append(",");
        sb.append("\"status\":").append(jsonString(rs.getString("status")));
        sb.append("}");
    }

    private static String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(value.length() + 2);
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
        return sb.toString();
    }

    static final class Queries {

        static final String TABLE_EXISTS_IN_SCHEMA =
          "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?)";

        static final String TABLE_EXISTS =
          "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ?)";

        final String locksTable;
        final String queueTable;
        final String allLocks;
        final String lockByName;
        final String allQueues;
        final String queueByName;
        final String messagesByQueue;
        final String messageById;

        Queries(String locksTable, String queueTable) {
            this.locksTable = locksTable;
            this.queueTable = queueTable;

            String statusExpr = """
                CASE WHEN locked_by IS NOT NULL AND locked_at IS NOT NULL AND (expires_at IS NULL OR expires_at > now())
                     THEN 'held'
                     WHEN locked_by IS NOT NULL AND locked_at IS NULL AND expires_at IS NOT NULL AND expires_at > now()
                     THEN 'quiet'
                     ELSE 'free' END AS status""";

            this.allLocks = """
                SELECT lock_name, token, locked_by, locked_at, expires_at,
                  %s
                FROM %s ORDER BY lock_name""".formatted(statusExpr, locksTable);

            this.lockByName = """
                SELECT lock_name, token, locked_by, locked_at, expires_at,
                  %s
                FROM %s WHERE lock_name = ?""".formatted(statusExpr, locksTable);

            this.allQueues = """
                SELECT queue_name,
                  COUNT(*) AS total,
                  COUNT(*) FILTER (WHERE visible_at <= now() AND picked_by IS NULL) AS visible,
                  COUNT(*) FILTER (WHERE picked_by IS NOT NULL) AS in_flight,
                  EXTRACT(EPOCH FROM now() - MIN(visible_at) FILTER (WHERE visible_at <= now())) AS oldest_age_seconds
                FROM %s GROUP BY queue_name ORDER BY queue_name""".formatted(queueTable);

            this.queueByName = """
                SELECT queue_name,
                  COUNT(*) AS total,
                  COUNT(*) FILTER (WHERE visible_at <= now() AND picked_by IS NULL) AS visible,
                  COUNT(*) FILTER (WHERE picked_by IS NOT NULL) AS in_flight,
                  EXTRACT(EPOCH FROM now() - MIN(visible_at) FILTER (WHERE visible_at <= now())) AS oldest_age_seconds
                FROM %s WHERE queue_name = ? GROUP BY queue_name""".formatted(queueTable);

            this.messagesByQueue = """
                SELECT id, encode(substring(payload from 1 for 200), 'base64') AS payload_preview,
                  type, picked_by, attempts, visible_at, created_at,
                  CASE WHEN picked_by IS NOT NULL THEN 'in_flight'
                       WHEN visible_at > now() THEN 'delayed'
                       ELSE 'visible' END AS status
                FROM %s WHERE queue_name = ? ORDER BY id LIMIT 100""".formatted(queueTable);

            this.messageById = """
                SELECT id, encode(payload, 'base64') AS payload_b64, type, headers,
                  picked_by, attempts, visible_at, created_at,
                  CASE WHEN picked_by IS NOT NULL THEN 'in_flight'
                       WHEN visible_at > now() THEN 'delayed'
                       ELSE 'visible' END AS status
                FROM %s WHERE queue_name = ? AND id = ?""".formatted(queueTable);
        }
    }
}
