package com.pivovarit.fencepost;

import com.pivovarit.fencepost.queue.Queue;
import com.pivovarit.fencepost.queue.QueuePublisher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class QueueAutoCommitIntegrationTest {

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:17");

    static DataSource rawDataSource;

    @BeforeAll
    static void setupDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        rawDataSource = ds;
    }

    @BeforeEach
    void createTable() throws SQLException {
        TestSchema.resetQueue(rawDataSource);
    }

    @Test
    void publishShouldRestoreAutoCommitBeforeReturningConnectionToPool() {
        RecordingDataSource dataSource = new RecordingDataSource(rawDataSource);
        QueuePublisher publisher = Fencepost.Queues.publisher(dataSource).build().forName("autocommit-publish");
        dataSource.autoCommitOnClose.clear();

        publisher.publish("hello".getBytes(UTF_8), "test.v1");

        assertThat(dataSource.autoCommitOnClose)
          .as("autoCommit state of each connection when returned to the pool")
          .isNotEmpty()
          .allMatch(Boolean::booleanValue);
    }

    @Test
    void enqueueShouldRestoreAutoCommitBeforeReturningConnectionToPool() {
        RecordingDataSource dataSource = new RecordingDataSource(rawDataSource);
        try (Queue queue = Fencepost.Queues.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build()
          .forName("autocommit-enqueue")) {
            dataSource.autoCommitOnClose.clear();

            queue.enqueue("hello".getBytes(UTF_8), "test.v1", Map.of());

            assertThat(dataSource.autoCommitOnClose)
              .as("autoCommit state of each connection when returned to the pool")
              .isNotEmpty()
              .allMatch(Boolean::booleanValue);
        }
    }

    /**
     * Simulates a pool that does not reset connection state on return (e.g. DBCP2
     * defaults): records each connection's autoCommit flag at close-time, which is
     * exactly the state the next borrower would observe.
     */
    private static final class RecordingDataSource implements DataSource {

        private final DataSource delegate;
        final List<Boolean> autoCommitOnClose = Collections.synchronizedList(new ArrayList<>());

        private RecordingDataSource(DataSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public Connection getConnection() throws SQLException {
            Connection conn = delegate.getConnection();
            return (Connection) Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[]{Connection.class},
              (proxy, method, args) -> {
                  if (method.getName().equals("close") && !conn.isClosed()) {
                      autoCommitOnClose.add(conn.getAutoCommit());
                  }
                  try {
                      return method.invoke(conn, args);
                  } catch (InvocationTargetException e) {
                      throw e.getCause();
                  }
              });
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return delegate.getLogWriter();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            delegate.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            delegate.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return delegate.getLoginTimeout();
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }
    }
}
