package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.LockFactory;
import com.pivovarit.fencepost.lock.RenewableLock;
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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class LeaseAutoRenewTimeoutIntegrationTest {

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

    @BeforeEach
    void createTables() throws SQLException {
        TestSchema.resetLocks(dataSource);
    }

    @Test
    void autoRenewShouldBoundEachRenewWithMillisecondPreciseStatementTimeout() {
        // lease=3s / renew=300ms — JDBC setQueryTimeout would floor this to a whole
        // second; the fix must enforce a sub-second, server-side statement_timeout so
        // worst-case loss detection stays inside the lease.
        List<String> recordedSql = new CopyOnWriteArrayList<>();
        DataSource recording = recordingPrepareStatements(dataSource, recordedSql);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(recording, Duration.ofSeconds(3))
          .withAutoRenew(Duration.ofMillis(300))
          .build();
        RenewableLock lock = provider.forName("ms-precise-renew");
        lock.lock();
        try {
            await().atMost(Duration.ofSeconds(2)).until(() ->
              recordedSql.stream().anyMatch(s -> s.startsWith("SET LOCAL statement_timeout")));
        } finally {
            lock.close();
        }

        long expected = LeaseLockInstance.autoRenewAttemptTimeoutMillis(3_000, 300);
        assertThat(timeoutsSetDuringRenew(recordedSql))
          .as("renew must set a millisecond-precise, sub-second statement_timeout")
          .isNotEmpty()
          .allSatisfy(ms -> assertThat(ms).isEqualTo(expected))
          .allSatisfy(ms -> assertThat(ms).isLessThan(1_000L));
    }

    @Test
    void autoRenewShouldBoundRenewConnectionsWithNetworkTimeout() {
        // statement_timeout only cancels the renew UPDATE server-side; if the TCP
        // connection is blackholed, getConnection/commit block at the OS retransmit
        // timeout. Each renew connection must carry a network timeout equal to the
        // per-attempt budget so the loss-detection bound holds without a healthy network.
        List<String> recordedSql = new CopyOnWriteArrayList<>();
        List<Integer> networkTimeouts = new CopyOnWriteArrayList<>();
        DataSource recording = recordingPrepareStatements(
          recordingNetworkTimeouts(dataSource, networkTimeouts), recordedSql);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(recording, Duration.ofSeconds(3))
          .withAutoRenew(Duration.ofMillis(300))
          .build();
        RenewableLock lock = provider.forName("network-timeout-renew");
        lock.lock();
        try {
            await().atMost(Duration.ofSeconds(2)).until(() ->
              recordedSql.stream().anyMatch(s -> s.startsWith("SET LOCAL statement_timeout")));
        } finally {
            lock.close();
        }

        int expected = (int) LeaseLockInstance.autoRenewAttemptTimeoutMillis(3_000, 300);
        assertThat(networkTimeouts.stream().filter(ms -> ms > 0))
          .as("renew connections must bound every network round trip with the per-attempt timeout")
          .isNotEmpty()
          .allSatisfy(ms -> assertThat(ms).isEqualTo(expected));
    }

    @Test
    void slowCheckoutShouldShrinkRenewStatementTimeoutToRemainingDetectionBudget() {
        // lease=6s / renew=600ms: per-attempt budget 600ms, cycle deadline 2100ms
        // (3 attempts + 300ms backoff). A pool checkout that eats 1600ms of the cycle
        // leaves only ~500ms until the deadline — the statement_timeout must shrink to
        // what is left, not assume checkout was free, or detection can overrun the lease.
        List<String> recordedSql = new CopyOnWriteArrayList<>();
        AtomicBoolean delayCheckouts = new AtomicBoolean(false);
        DataSource slow = delayingCheckouts(dataSource, delayCheckouts, 1_600, new AtomicInteger());
        DataSource recording = recordingPrepareStatements(slow, recordedSql);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(recording, Duration.ofSeconds(6))
          .withAutoRenew(Duration.ofMillis(600))
          .build();
        RenewableLock lock = provider.forName("slow-checkout-renew");
        lock.lock();
        delayCheckouts.set(true);
        try {
            await().atMost(Duration.ofSeconds(8)).until(() ->
              !timeoutsSetDuringRenew(recordedSql).isEmpty());
        } finally {
            delayCheckouts.set(false);
            lock.close();
        }

        long cycleBudget = LeaseLockInstance.autoRenewCycleBudgetMillis(6_000, 600);
        assertThat(timeoutsSetDuringRenew(recordedSql))
          .as("renew statement_timeout must be charged for the 1600ms pool checkout")
          .isNotEmpty()
          .allSatisfy(ms -> assertThat(ms).isBetween(1L, cycleBudget - 1_600));
    }

    @Test
    void checkoutBlockingPastDetectionDeadlineShouldReportLossWithoutRetrying() {
        // A checkout that blocks past the whole 2100ms cycle deadline must fail the
        // renew immediately when it returns: no SQL on a blown budget, no further
        // retries (each would block another 2500ms in the pool), loss reported.
        List<String> recordedSql = new CopyOnWriteArrayList<>();
        AtomicBoolean delayCheckouts = new AtomicBoolean(false);
        AtomicInteger delayedCheckouts = new AtomicInteger();
        DataSource slow = delayingCheckouts(dataSource, delayCheckouts, 2_500, delayedCheckouts);
        DataSource recording = recordingPrepareStatements(slow, recordedSql);
        AtomicReference<FencepostException> reported = new AtomicReference<>();

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(recording, Duration.ofSeconds(6))
          .withAutoRenew(Duration.ofMillis(600))
          .onAutoRenewFailure(reported::set)
          .build();
        RenewableLock lock = provider.forName("blocked-checkout-renew");
        lock.lock();
        delayCheckouts.set(true);
        try {
            await().atMost(Duration.ofSeconds(8)).until(() -> reported.get() != null);

            assertThat(delayedCheckouts.get())
              .as("a checkout that blew the cycle deadline must not be retried")
              .isEqualTo(1);
            assertThat(timeoutsSetDuringRenew(recordedSql))
              .as("no renew statement may run once the detection deadline has passed")
              .isEmpty();
        } finally {
            delayCheckouts.set(false);
            lock.close();
        }
    }

    private static DataSource delayingCheckouts(DataSource real, AtomicBoolean enabled, long delayMillis, AtomicInteger delayed) {
        return (DataSource) Proxy.newProxyInstance(
          DataSource.class.getClassLoader(),
          new Class<?>[]{DataSource.class},
          (proxy, method, args) -> {
              if ("getConnection".equals(method.getName()) && enabled.get()) {
                  delayed.incrementAndGet();
                  try {
                      Thread.sleep(delayMillis);
                  } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new SQLException("interrupted while checking out connection", e);
                  }
              }
              return invoke(real, method, args);
          });
    }

    private static List<Long> timeoutsSetDuringRenew(List<String> recordedSql) {
        Pattern p = Pattern.compile("SET LOCAL statement_timeout = '(\\d+)ms'");
        List<Long> timeouts = new CopyOnWriteArrayList<>();
        for (String sql : recordedSql) {
            Matcher m = p.matcher(sql);
            if (m.find()) {
                timeouts.add(Long.parseLong(m.group(1)));
            }
        }
        return timeouts;
    }

    private static DataSource recordingPrepareStatements(DataSource real, List<String> sink) {
        return (DataSource) Proxy.newProxyInstance(
          DataSource.class.getClassLoader(),
          new Class<?>[]{DataSource.class},
          (proxy, method, args) -> {
              if ("getConnection".equals(method.getName())) {
                  return recordingConnection((Connection) invoke(real, method, args), sink);
              }
              return invoke(real, method, args);
          });
    }

    private static Connection recordingConnection(Connection real, List<String> sink) {
        return (Connection) Proxy.newProxyInstance(
          Connection.class.getClassLoader(),
          new Class<?>[]{Connection.class},
          (proxy, method, args) -> {
              if ("prepareStatement".equals(method.getName()) && args != null && args.length > 0
                && args[0] instanceof String sql) {
                  sink.add(sql);
              }
              return invoke(real, method, args);
          });
    }

    private static DataSource recordingNetworkTimeouts(DataSource real, List<Integer> sink) {
        return (DataSource) Proxy.newProxyInstance(
          DataSource.class.getClassLoader(),
          new Class<?>[]{DataSource.class},
          (proxy, method, args) -> {
              if ("getConnection".equals(method.getName())) {
                  Connection connection = (Connection) invoke(real, method, args);
                  return (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class},
                    (connProxy, connMethod, connArgs) -> {
                        if ("setNetworkTimeout".equals(connMethod.getName()) && connArgs != null && connArgs.length == 2) {
                            sink.add((Integer) connArgs[1]);
                        }
                        return invoke(connection, connMethod, connArgs);
                    });
              }
              return invoke(real, method, args);
          });
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
