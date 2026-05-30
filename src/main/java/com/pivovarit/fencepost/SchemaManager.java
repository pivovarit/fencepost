package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

final class SchemaManager {

    private SchemaManager() {
    }

    static void createLockSchema(DataSource dataSource, String tableName) {
        String sql = """
            CREATE TABLE IF NOT EXISTS %s (
              lock_name TEXT PRIMARY KEY,
              lock_type TEXT NOT NULL,
              token BIGINT NOT NULL DEFAULT 0,
              locked_by TEXT,
              locked_at TIMESTAMP WITH TIME ZONE,
              expires_at TIMESTAMP WITH TIME ZONE
            )""".formatted(tableName);
        executeSql(dataSource, sql);
    }

    static void createQueueSchema(DataSource dataSource, String tableName) {
        String indexName = "idx_" + bareTableName(tableName) + "_dequeue";
        String sql = """
            CREATE TABLE IF NOT EXISTS %s (
              id BIGSERIAL PRIMARY KEY,
              queue_name TEXT NOT NULL,
              payload BYTEA NOT NULL,
              type TEXT,
              headers JSONB,
              created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
              visible_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
              attempts INT NOT NULL DEFAULT 0,
              picked_by TEXT
            );
            CREATE INDEX IF NOT EXISTS %s ON %s (queue_name, visible_at)""".formatted(tableName, indexName, tableName);
        executeSql(dataSource, sql);
    }

    private static final int MAX_DDL_ATTEMPTS = 5;

    private static void executeSql(DataSource dataSource, String sql) {
        for (int attempt = 1; ; attempt++) {
            try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
                conn.setAutoCommit(true);
                stmt.execute(sql);
                return;
            } catch (SQLException e) {
                String state = e.getSQLState();
                if (SqlStates.UNIQUE_VIOLATION.equals(state) || SqlStates.DUPLICATE_TABLE.equals(state) || SqlStates.DUPLICATE_OBJECT.equals(state)) {
                    return;
                }
                // transient race, retry
                if (SqlStates.TUPLE_CONCURRENTLY_UPDATED.equals(state) && attempt < MAX_DDL_ATTEMPTS) {
                    continue;
                }
                throw new FencepostException("Failed to execute schema SQL", e);
            }
        }
    }

    static String bareTableName(String tableName) {
        int dot = tableName.lastIndexOf('.');
        return dot == -1 ? tableName : tableName.substring(dot + 1);
    }

    static String schemaName(String tableName) {
        int dot = tableName.lastIndexOf('.');
        return dot == -1 ? "public" : tableName.substring(0, dot);
    }

    static void validateLockSchema(DataSource dataSource, String tableName) {
        String schema = schemaName(tableName);
        String bare = bareTableName(tableName);

        validateTable(dataSource, schema, bare, Arrays.asList(
            new String[]{"lock_name", "text"},
            new String[]{"lock_type", "text"},
            new String[]{"token", "bigint"},
            new String[]{"locked_by", "text"},
            new String[]{"locked_at", "timestamp with time zone"},
            new String[]{"expires_at", "timestamp with time zone"}
        ));
    }

    static void validateQueueSchema(DataSource dataSource, String tableName) {
        String schema = schemaName(tableName);
        String bare = bareTableName(tableName);

        validateTable(dataSource, schema, bare, Arrays.asList(
            new String[]{"id", "bigint"},
            new String[]{"queue_name", "text"},
            new String[]{"payload", "bytea"},
            new String[]{"type", "text"},
            new String[]{"headers", "jsonb"},
            new String[]{"created_at", "timestamp with time zone"},
            new String[]{"visible_at", "timestamp with time zone"},
            new String[]{"attempts", "integer"},
            new String[]{"picked_by", "text"}
        ));
    }

    private static void validateTable(DataSource dataSource, String schema, String table, List<String[]> expectedColumns) {
        String schemaFolded = schema.toLowerCase(Locale.ROOT);
        String tableFolded = table.toLowerCase(Locale.ROOT);
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")) {
                ps.setString(1, schemaFolded);
                ps.setString(2, tableFolded);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new FencepostException("Required table '" + table + "' does not exist");
                    }
                }
            }

            for (String[] col : expectedColumns) {
                String colName = col[0];
                String colType = col[1];
                try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?")) {
                    ps.setString(1, schemaFolded);
                    ps.setString(2, tableFolded);
                    ps.setString(3, colName);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            throw new FencepostException("Table '" + table + "' is missing column '" + colName + "' (expected type: " + colType + ")");
                        }
                        String actualType = rs.getString(1);
                        if (!colType.equals(actualType)) {
                            throw new FencepostException("Table '" + table + "' column '" + colName + "' has type '" + actualType + "', expected '" + colType + "'");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new FencepostException("Failed to validate schema", e);
        }
    }

}
