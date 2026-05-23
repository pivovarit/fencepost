package com.pivovarit.fencepost;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class ListenerConnectionIntegrationTest {

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:17");

    static DataSource dataSource;

    @BeforeAll
    static void setupDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        dataSource = ds;
    }

    @Test
    void ensureShouldReturnOpenConnection() throws SQLException {
        ListenerConnection listener = new ListenerConnection(dataSource, "test_channel");

        Connection conn = listener.ensure();

        assertThat(conn).isNotNull();
        assertThat(conn.isClosed()).isFalse();

        listener.stop();
    }

    @Test
    void ensureShouldReturnSameConnectionOnSubsequentCalls() throws SQLException {
        ListenerConnection listener = new ListenerConnection(dataSource, "test_reuse");

        Connection first = listener.ensure();
        Connection second = listener.ensure();

        assertThat(first).isSameAs(second);

        listener.stop();
    }

    @Test
    void ensureShouldReconnectAfterConnectionClosed() throws SQLException {
        ListenerConnection listener = new ListenerConnection(dataSource, "test_reconnect");

        Connection first = listener.ensure();
        first.close();

        Connection second = listener.ensure();

        assertThat(second).isNotSameAs(first);
        assertThat(second.isClosed()).isFalse();

        listener.stop();
    }

    @Test
    void ensureShouldThrowAfterStop() throws SQLException {
        ListenerConnection listener = new ListenerConnection(dataSource, "test_stop");

        listener.ensure();
        listener.stop();

        assertThatThrownBy(listener::ensure)
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("stopped");
    }

    @Test
    void stopShouldCloseConnection() throws SQLException {
        ListenerConnection listener = new ListenerConnection(dataSource, "test_stop_close");

        Connection conn = listener.ensure();
        assertThat(conn.isClosed()).isFalse();

        listener.stop();

        assertThat(conn.isClosed()).isTrue();
    }

    @Test
    void stopShouldBeIdempotent() throws SQLException {
        ListenerConnection listener = new ListenerConnection(dataSource, "test_stop_idempotent");

        listener.ensure();
        listener.stop();
        listener.stop();
    }

    @Test
    void closeShouldCloseConnectionAndAllowNewEnsure() throws SQLException {
        ListenerConnection listener = new ListenerConnection(dataSource, "test_close_reopen");

        Connection first = listener.ensure();
        listener.close();
        assertThat(first.isClosed()).isTrue();

        Connection second = listener.ensure();
        assertThat(second.isClosed()).isFalse();
        assertThat(second).isNotSameAs(first);

        listener.stop();
    }

    @Test
    void closeWithoutEnsureShouldBeNoOp() {
        ListenerConnection listener = new ListenerConnection(dataSource, "test_close_noop");

        listener.close();
    }

    @Test
    void isStoppedShouldReflectState() throws SQLException {
        ListenerConnection listener = new ListenerConnection(dataSource, "test_is_stopped");

        assertThat(listener.isStopped()).isFalse();
        listener.ensure();
        assertThat(listener.isStopped()).isFalse();
        listener.stop();
        assertThat(listener.isStopped()).isTrue();
    }

    @Test
    void ensureShouldSetAutoCommitToTrue() throws SQLException {
        ListenerConnection listener = new ListenerConnection(dataSource, "test_autocommit");

        Connection conn = listener.ensure();

        assertThat(conn.getAutoCommit()).isTrue();

        listener.stop();
    }

    @Test
    void concurrentEnsureShouldReturnValidConnection() throws Exception {
        ListenerConnection listener = new ListenerConnection(dataSource, "test_concurrent");

        Thread[] threads = new Thread[5];
        Connection[] connections = new Connection[5];
        Exception[] errors = new Exception[5];

        for (int i = 0; i < threads.length; i++) {
            int idx = i;
            threads[i] = new Thread(() -> {
                try {
                    connections[idx] = listener.ensure();
                } catch (Exception e) {
                    errors[idx] = e;
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join(5000);

        for (Exception error : errors) {
            assertThat(error).isNull();
        }
        for (Connection conn : connections) {
            assertThat(conn).isNotNull();
            assertThat(conn.isClosed()).isFalse();
        }

        listener.stop();
    }

    @Test
    void stopDuringEnsureShouldNotLeakConnection() throws Exception {
        FaultToleranceTest.FaultDataSource faultDs = new FaultToleranceTest.FaultDataSource(dataSource);
        ListenerConnection listener = new ListenerConnection(faultDs, "test_stop_during");

        Connection conn = listener.ensure();
        assertThat(conn.isClosed()).isFalse();

        listener.stop();
        assertThat(conn.isClosed()).isTrue();

        assertThatThrownBy(listener::ensure)
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("stopped");
    }
}
