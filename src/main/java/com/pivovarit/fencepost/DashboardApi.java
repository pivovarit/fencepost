package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.sql.SQLException;

public final class DashboardApi {

    private final DataSource dataSource;
    private final String locksTable;
    private final String queueTable;

    public DashboardApi(DataSource dataSource, String locksTable, String queueTable) {
        this.dataSource = dataSource;
        this.locksTable = locksTable;
        this.queueTable = queueTable;
    }

    public String status() throws SQLException {
        boolean locksEnabled = tableExists(locksTable);
        boolean queuesEnabled = tableExists(queueTable);
        return "{\"locks_enabled\":" + locksEnabled + ",\"queues_enabled\":" + queuesEnabled + "}";
    }

    public String locks() throws SQLException {
        return Jdbc.query(dataSource, Queries.allLocks(locksTable)).map(rs -> {            StringBuilder sb = new StringBuilder("[");
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
        return Jdbc.query(dataSource, Queries.lockByName(locksTable)).bind(name).map(rs -> {
            if (!rs.next()) {
                return "null";
            }
            StringBuilder sb = new StringBuilder();
            appendLockRow(sb, rs);
            return sb.toString();
        });
    }

    public String queues() throws SQLException {
        return Jdbc.query(dataSource, Queries.allQueues(queueTable)).map(rs -> {
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
        String summaryJson = Jdbc.query(dataSource, Queries.queueByName(queueTable)).bind(name).map(rs -> {
            if (!rs.next()) {
                return "{\"name\":" + jsonString(name) +
                       ",\"total\":0,\"visible\":0,\"in_flight\":0,\"oldest_age_seconds\":null";
            }
            StringBuilder sb = new StringBuilder();
            appendQueueSummaryRow(sb, rs);
            return sb.substring(0, sb.length() - 1);
        });

        String messagesJson = Jdbc.query(dataSource, Queries.messagesByQueue(queueTable)).bind(name).map(rs -> {
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

    public String message(String queueName, long id) throws SQLException {
        return Jdbc.query(dataSource, Queries.messageById(queueTable)).bind(queueName).bind(id).map(rs -> {
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

    private boolean tableExists(String tableName) throws SQLException {
        String[] parts = tableName.split("\\.", 2);
        if (parts.length == 2) {
            String schema = parts[0];
            String table = parts[1];
            return Jdbc.query(dataSource, Queries.TABLE_EXISTS_IN_SCHEMA).bind(schema).bind(table).map(rs -> {
                rs.next();
                return rs.getBoolean(1);
            });
        } else {
            return Jdbc.query(dataSource, Queries.TABLE_EXISTS).bind(tableName).map(rs -> {
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
        sb.append("\"is_held\":").append(rs.getBoolean("is_held"));
        sb.append("}");
    }

    private static void appendQueueSummaryRow(StringBuilder sb, java.sql.ResultSet rs) throws SQLException {
        sb.append("{");
        sb.append("\"name\":").append(jsonString(rs.getString("queue_name"))).append(",");
        sb.append("\"total\":").append(rs.getLong("total")).append(",");
        sb.append("\"visible\":").append(rs.getLong("visible")).append(",");
        sb.append("\"in_flight\":").append(rs.getLong("in_flight")).append(",");
        Object age = rs.getObject("oldest_age_seconds");
        sb.append("\"oldest_age_seconds\":").append(age == null ? "null" : age.toString());
        sb.append("}");
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
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    static final class Queries {

        static final String TABLE_EXISTS_IN_SCHEMA =
          "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?)";

        static final String TABLE_EXISTS =
          "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ?)";

        static String allLocks(String table) {
            return "SELECT lock_name, token, locked_by, locked_at, expires_at, " +
              "  CASE WHEN locked_by IS NOT NULL AND (expires_at IS NULL OR expires_at > now()) " +
              "       THEN true ELSE false END AS is_held " +
              "FROM " + table + " ORDER BY lock_name";
        }

        static String lockByName(String table) {
            return "SELECT lock_name, token, locked_by, locked_at, expires_at, " +
              "  CASE WHEN locked_by IS NOT NULL AND (expires_at IS NULL OR expires_at > now()) " +
              "       THEN true ELSE false END AS is_held " +
              "FROM " + table + " WHERE lock_name = ?";
        }

        static String allQueues(String table) {
            return "SELECT queue_name, " +
              "  COUNT(*) AS total, " +
              "  COUNT(*) FILTER (WHERE visible_at <= now() AND picked_by IS NULL) AS visible, " +
              "  COUNT(*) FILTER (WHERE picked_by IS NOT NULL) AS in_flight, " +
              "  EXTRACT(EPOCH FROM now() - MIN(visible_at) FILTER (WHERE visible_at <= now())) AS oldest_age_seconds " +
              "FROM " + table + " GROUP BY queue_name ORDER BY queue_name";
        }

        static String queueByName(String table) {
            return "SELECT queue_name, " +
              "  COUNT(*) AS total, " +
              "  COUNT(*) FILTER (WHERE visible_at <= now() AND picked_by IS NULL) AS visible, " +
              "  COUNT(*) FILTER (WHERE picked_by IS NOT NULL) AS in_flight, " +
              "  EXTRACT(EPOCH FROM now() - MIN(visible_at) FILTER (WHERE visible_at <= now())) AS oldest_age_seconds " +
              "FROM " + table + " WHERE queue_name = ? GROUP BY queue_name";
        }

        static String messagesByQueue(String table) {
            return "SELECT id, encode(substring(payload from 1 for 200), 'base64') AS payload_preview, type, picked_by, attempts, visible_at, created_at, " +
              "  CASE WHEN picked_by IS NOT NULL THEN 'in_flight' " +
              "       WHEN visible_at > now() THEN 'delayed' " +
              "       ELSE 'visible' END AS status " +
              "FROM " + table + " WHERE queue_name = ? ORDER BY id";
        }

        static String messageById(String table) {
            return "SELECT id, encode(payload, 'base64') AS payload_b64, type, headers, picked_by, attempts, visible_at, created_at, " +
              "  CASE WHEN picked_by IS NOT NULL THEN 'in_flight' " +
              "       WHEN visible_at > now() THEN 'delayed' " +
              "       ELSE 'visible' END AS status " +
              "FROM " + table + " WHERE queue_name = ? AND id = ?";
        }
    }
}
