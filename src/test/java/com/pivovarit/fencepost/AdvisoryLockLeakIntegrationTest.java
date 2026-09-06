package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.AdvisoryLock;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class AdvisoryLockLeakIntegrationTest {

    @Container
    private static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    @Test
    void failedAcquisitionMustNotLeakAnAlreadyGrantedLockIntoThePool() throws SQLException {
        try (HikariDataSource pool = singleConnectionPool()) {
            DataSource cancellingAfterGrant = cancelAfterServerSideGrant(pool);
            AdvisoryLock lock = new AdvisoryLockInstance("leak-test-lock", cancellingAfterGrant);

            assertThatThrownBy(lock::lock).isInstanceOf(FencepostException.class);

            assertThat(advisoryLocksHeldByPooledSession(pool))
              .as("the physical connection returned to the pool must not still hold the advisory lock")
              .isZero();
        }
    }

    private static int advisoryLocksHeldByPooledSession(DataSource dataSource) throws SQLException {
        try (Connection c = dataSource.getConnection();
             Statement statement = c.createStatement();
             ResultSet rs = statement.executeQuery(
               "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static DataSource cancelAfterServerSideGrant(DataSource delegate) {
        return (DataSource) Proxy.newProxyInstance(
          DataSource.class.getClassLoader(),
          new Class<?>[] {DataSource.class},
          (proxy, method, args) -> {
              if ("getConnection".equals(method.getName()) && (args == null || args.length == 0)) {
                  return cancellingConnection(delegate.getConnection());
              }
              return invokeReal(method, delegate, args);
          });
    }

    private static Connection cancellingConnection(Connection real) {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("prepareStatement".equals(method.getName()) && args != null && args.length >= 1
              && args[0] instanceof String sql && sql.contains("pg_advisory_lock(")) {
                PreparedStatement realStatement = (PreparedStatement) invokeReal(method, real, args);
                return cancellingStatement(realStatement);
            }
            return invokeReal(method, real, args);
        };
        return (Connection) Proxy.newProxyInstance(
          Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }

    private static PreparedStatement cancellingStatement(PreparedStatement real) {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("executeQuery".equals(method.getName())) {
                try (ResultSet ignored = (ResultSet) invokeReal(method, real, args)) {
                    // server has now granted the advisory lock
                }
                throw new SQLException("simulated cancellation after server-side grant", "57014");
            }
            return invokeReal(method, real, args);
        };
        return (PreparedStatement) Proxy.newProxyInstance(
          PreparedStatement.class.getClassLoader(), new Class<?>[] {PreparedStatement.class}, handler);
    }

    private static Object invokeReal(Method method, Object target, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static HikariDataSource singleConnectionPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(1);
        return new HikariDataSource(config);
    }
}
