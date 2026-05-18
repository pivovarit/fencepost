package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

final class SchemaManager {

    private SchemaManager() {
    }

    static void createLockSchema(DataSource dataSource, String tableName) {
        String tokenTable = TableBasedLock.tokenTableName(tableName);
        String tokenSeq = TableBasedLock.tokenSequenceName(tableName);
        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
            + "lock_name TEXT PRIMARY KEY,"
            + "lock_type TEXT NOT NULL,"
            + "token BIGINT NOT NULL DEFAULT 0,"
            + "locked_by TEXT,"
            + "locked_at TIMESTAMP WITH TIME ZONE,"
            + "expires_at TIMESTAMP WITH TIME ZONE"
            + ");"
            + "CREATE TABLE IF NOT EXISTS " + tokenTable + " ("
            + "lock_name TEXT PRIMARY KEY,"
            + "token BIGINT NOT NULL DEFAULT 0,"
            + "last_locked_by TEXT,"
            + "last_locked_at TIMESTAMP WITH TIME ZONE"
            + ");"
            + "CREATE SEQUENCE IF NOT EXISTS " + tokenSeq;
        executeSql(dataSource, sql);
    }

    static void createQueueSchema(DataSource dataSource, String tableName) {
        String indexName = "idx_" + bareTableName(tableName) + "_dequeue";
        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
            + "id BIGSERIAL PRIMARY KEY,"
            + "queue_name TEXT NOT NULL,"
            + "payload BYTEA NOT NULL,"
            + "type TEXT,"
            + "headers JSONB,"
            + "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),"
            + "visible_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),"
            + "attempts INT NOT NULL DEFAULT 0,"
            + "picked_by TEXT"
            + ");"
            + "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + tableName + " (queue_name, visible_at)";
        executeSql(dataSource, sql);
    }

    private static void executeSql(DataSource dataSource, String sql) {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new FencepostException("Failed to execute schema SQL", e);
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
        String tokenTable = TableBasedLock.tokenTableName(tableName);
        String tokenSeq = TableBasedLock.tokenSequenceName(tableName);
        String schema = schemaName(tableName);
        String bare = bareTableName(tableName);
        String bareToken = bareTableName(tokenTable);

        validateTable(dataSource, schema, bare, Arrays.asList(
            new String[]{"lock_name", "text"},
            new String[]{"lock_type", "text"},
            new String[]{"token", "bigint"},
            new String[]{"locked_by", "text"},
            new String[]{"locked_at", "timestamp with time zone"},
            new String[]{"expires_at", "timestamp with time zone"}
        ));
        validateTable(dataSource, schema, bareToken, Arrays.asList(
            new String[]{"lock_name", "text"},
            new String[]{"token", "bigint"},
            new String[]{"last_locked_by", "text"},
            new String[]{"last_locked_at", "timestamp with time zone"}
        ));
        validateSequence(dataSource, schema, bareTableName(tokenSeq));
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
        try (Connection conn = dataSource.getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = '" + schema + "' AND table_name = '" + table + "'");
            if (!rs.next()) {
                throw new FencepostException("Required table '" + table + "' does not exist");
            }

            for (String[] col : expectedColumns) {
                String colName = col[0];
                String colType = col[1];
                rs = conn.createStatement().executeQuery(
                    "SELECT data_type FROM information_schema.columns WHERE table_schema = '" + schema
                        + "' AND table_name = '" + table
                        + "' AND column_name = '" + colName + "'");
                if (!rs.next()) {
                    throw new FencepostException("Table '" + table + "' is missing column '" + colName + "' (expected type: " + colType + ")");
                }
                String actualType = rs.getString(1);
                if (!colType.equals(actualType)) {
                    throw new FencepostException("Table '" + table + "' column '" + colName + "' has type '" + actualType + "', expected '" + colType + "'");
                }
            }
        } catch (SQLException e) {
            throw new FencepostException("Failed to validate schema", e);
        }
    }

    private static void validateSequence(DataSource dataSource, String schema, String sequence) {
        try (Connection conn = dataSource.getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT 1 FROM pg_sequences WHERE schemaname = '" + schema + "' AND sequencename = '" + sequence + "'");
            if (!rs.next()) {
                throw new FencepostException("Required sequence '" + sequence + "' does not exist");
            }
        } catch (SQLException e) {
            throw new FencepostException("Failed to validate schema", e);
        }
    }
}
