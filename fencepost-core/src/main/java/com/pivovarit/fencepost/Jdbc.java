package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("SqlSourceToSinkFlow")
final class Jdbc {

    private Jdbc() {
    }

    @FunctionalInterface
    public interface ResultSetMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    public static Query query(DataSource ds, String sql) {
        return new Query(ds, null, sql);
    }

    public static Query query(Connection conn, String sql) {
        return new Query(null, conn, sql);
    }

    static Update update(DataSource ds, String sql) {
        return new Update(ds, null, sql);
    }

    static Update update(Connection conn, String sql) {
        return new Update(null, conn, sql);
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

    static final class Query {
        private final DataSource ds;
        private final Connection conn;
        private final String sql;
        private final List<Object> params = new ArrayList<>();

        private Query(DataSource ds, Connection conn, String sql) {
            this.ds = ds;
            this.conn = conn;
            this.sql = sql;
        }

        Query bind(Object value) {
            params.add(value);
            return this;
        }

        <T> T map(ResultSetMapper<T> mapper) throws SQLException {
            if (ds != null) {
                try (Connection c = ds.getConnection()) {
                    return execute(c, mapper);
                }
            }
            return execute(conn, mapper);
        }

        private <T> T execute(Connection c, ResultSetMapper<T> mapper) throws SQLException {
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                bindAll(ps, params);
                try (ResultSet rs = ps.executeQuery()) {
                    return mapper.map(rs);
                }
            }
        }
    }

    static final class Update {
        private final DataSource ds;
        private final Connection conn;
        private final String sql;
        private final List<Object> params = new ArrayList<>();

        private Update(DataSource ds, Connection conn, String sql) {
            this.ds = ds;
            this.conn = conn;
            this.sql = sql;
        }

        Update bind(Object value) {
            params.add(value);
            return this;
        }

        int execute() throws SQLException {
            if (ds != null) {
                try (Connection c = ds.getConnection()) {
                    return execute(c);
                }
            }
            return execute(conn);
        }

        private int execute(Connection c) throws SQLException {
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                bindAll(ps, params);
                return ps.executeUpdate();
            }
        }
    }

    private static void bindAll(PreparedStatement ps, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            ps.setObject(i + 1, params.get(i));
        }
    }
}
