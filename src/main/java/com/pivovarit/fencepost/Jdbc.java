package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

@SuppressWarnings("SqlSourceToSinkFlow")
final class Jdbc {

    private Jdbc() {
    }

    @FunctionalInterface
    interface ResultSetMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    static <T> T query(DataSource ds, String sql, ResultSetMapper<T> mapper, Object... params) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            return query(conn, sql, mapper, params);
        }
    }

    static <T> T query(Connection conn, String sql, ResultSetMapper<T> mapper, Object... params) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                return mapper.map(rs);
            }
        }
    }

    static int update(DataSource ds, String sql, Object... params) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            return update(conn, sql, params);
        }
    }

    static int update(Connection conn, String sql, Object... params) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bind(ps, params);
            return ps.executeUpdate();
        }
    }

    static void execute(Connection conn, String sql) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.execute();
        }
    }

    static void setStatementTimeout(Connection conn, Duration timeout) throws SQLException {
        execute(conn, String.format("SET LOCAL statement_timeout = '%dms'", timeout.toMillis()));
    }

    static void resetStatementTimeout(Connection conn) throws SQLException {
        execute(conn, "SET LOCAL statement_timeout = 0");
    }

    static void setLockTimeout(Connection conn, Duration timeout) throws SQLException {
        execute(conn, String.format("SET lock_timeout = '%dms'", timeout.toMillis()));
    }

    static void resetLockTimeout(Connection conn) throws SQLException {
        execute(conn, "SET lock_timeout = 0");
    }

    static String intervalMillis() {
        return "? * interval '1 millisecond'";
    }

    private static void bind(PreparedStatement ps, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            ps.setObject(i + 1, params[i]);
        }
    }
}
