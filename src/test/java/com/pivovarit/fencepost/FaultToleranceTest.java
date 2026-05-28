package com.pivovarit.fencepost;

import com.pivovarit.fencepost.election.LeaderElection;
import com.pivovarit.fencepost.lock.FencedLock;
import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.LockFactory;
import com.pivovarit.fencepost.lock.RenewableLock;
import com.pivovarit.fencepost.queue.AckUnknownException;
import com.pivovarit.fencepost.queue.Message;
import com.pivovarit.fencepost.queue.Queue;
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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@Testcontainers
class FaultToleranceTest {

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
        TestSchema.resetQueue(dataSource);
    }

    @Test
    void leaseAutoRenewShouldFireCallbackAfterPersistentConnectionFailure() throws Exception {
        FaultDataSource faultDs = new FaultDataSource(dataSource);
        CountDownLatch callbackFired = new CountDownLatch(1);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(faultDs, Duration.ofSeconds(2))
          .withAutoRenew(Duration.ofMillis(200))
          .onAutoRenewFailure(ex -> callbackFired.countDown())
          .build();

        RenewableLock lock = provider.forName("renew-fail-test");
        lock.lock();

        faultDs.startFailing();

        assertThat(callbackFired.await(10, TimeUnit.SECONDS))
          .as("onAutoRenewFailure should fire after retries are exhausted")
          .isTrue();

        faultDs.stopFailing();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            RenewableLock contender = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(2)).build()
              .forName("renew-fail-test");
            assertThat(contender.tryLock())
              .as("lock should be acquirable after lease expiry due to auto-renew failure")
              .isPresent();
            contender.unlock();
        });

        lock.close();
    }

    @Test
    void leaseAutoRenewShouldRecoverFromTransientConnectionFailure() throws Exception {
        FaultDataSource faultDs = new FaultDataSource(dataSource);
        AtomicBoolean callbackFired = new AtomicBoolean(false);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(faultDs, Duration.ofSeconds(5))
          .withAutoRenew(Duration.ofMillis(200))
          .onAutoRenewFailure(ex -> callbackFired.set(true))
          .build();

        RenewableLock lock = provider.forName("transient-renew-test");
        lock.lock();

        faultDs.failNext(2);

        Thread.sleep(2000);

        assertThat(callbackFired.get())
          .as("onAutoRenewFailure should not fire when retries recover from transient failure")
          .isFalse();

        lock.unlock();
    }

    @Test
    void leaseUnlockFailureShouldNotPreventLeaseExpiry() {
        FaultDataSource faultDs = new FaultDataSource(dataSource);

        LockFactory<RenewableLock> provider = Fencepost.Locks.lease(faultDs, Duration.ofSeconds(1)).build();
        RenewableLock lock = provider.forName("unlock-fail-lease");
        lock.lock();

        faultDs.startFailing();

        assertThatThrownBy(lock::unlock)
          .isInstanceOf(FencepostException.class);

        faultDs.stopFailing();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            RenewableLock contender = Fencepost.Locks.lease(dataSource, Duration.ofSeconds(1)).build()
              .forName("unlock-fail-lease");
            assertThat(contender.tryLock())
              .as("lock should be acquirable after lease expiry despite failed unlock")
              .isPresent();
            contender.unlock();
        });
    }

    @Test
    void sessionLockShouldReleaseWhenConnectionFailsDuringTokenAllocation() {
        DataSource faultyDs = failOnSqlContaining(dataSource, "nextval");

        LockFactory<FencedLock> provider = Fencepost.Locks.session(faultyDs).build();
        FencedLock lock = provider.forName("token-alloc-fail");

        assertThatThrownBy(lock::lock)
          .isInstanceOf(FencepostException.class);

        FencedLock contender = Fencepost.Locks.session(dataSource).build().forName("token-alloc-fail");
        Optional<FencingToken> token = contender.tryLock();
        assertThat(token)
          .as("lock should be acquirable after failed token allocation released the connection")
          .isPresent();
        contender.unlock();
    }

    @Test
    void sessionUnlockShouldRestoreAutoCommitBeforeReturningConnection() {
        List<Boolean> autoCommitAtClose = new CopyOnWriteArrayList<>();
        DataSource recordingDs = recordingAutoCommitOnClose(dataSource, autoCommitAtClose);

        FencedLock lock = Fencepost.Locks.session(recordingDs).build().forName("autocommit-restore-unlock");
        lock.lock();
        lock.unlock();

        assertThat(autoCommitAtClose)
          .as("connection returned to the pool after unlock should have autoCommit restored to true")
          .isNotEmpty()
          .containsOnly(true);
    }

    @Test
    void sessionLockShouldRestoreAutoCommitWhenAcquisitionFails() {
        List<Boolean> autoCommitAtClose = new CopyOnWriteArrayList<>();
        DataSource recordingDs = recordingAutoCommitOnClose(
          failOnSqlContaining(dataSource, "nextval"), autoCommitAtClose);

        FencedLock lock = Fencepost.Locks.session(recordingDs).build().forName("autocommit-restore-fail");

        assertThatThrownBy(lock::lock)
          .isInstanceOf(FencepostException.class);

        assertThat(autoCommitAtClose)
          .as("connection returned to the pool after a failed acquisition should have autoCommit restored to true")
          .isNotEmpty()
          .containsOnly(true);
    }

    @Test
    void enqueueShouldLeaveNoTraceOnCommitFailure() {
        DataSource faultyDs = failOnCommit(dataSource);
        Queue queue = Fencepost.Queues.queue(faultyDs)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build()
          .forName("commit-fail-queue");

        assertThatThrownBy(() -> queue.enqueue("should-not-persist".getBytes(UTF_8), "test", Map.of()))
          .isInstanceOf(FencepostException.class);

        Queue healthyQueue = Fencepost.Queues.queue(dataSource)
          .visibilityTimeout(Duration.ofMinutes(5))
          .build()
          .forName("commit-fail-queue");

        assertThat(healthyQueue.tryDequeue())
          .as("no message should be left in queue after commit failure")
          .isEmpty();
    }

    @Test
    void dequeuedMessageShouldBeRedeliveredWhenAckFailsAndVisibilityExpires() throws Exception {
        FaultDataSource faultDs = new FaultDataSource(dataSource);
        Queue queue = Fencepost.Queues.queue(faultDs)
          .visibilityTimeout(Duration.ofSeconds(1))
          .build()
          .forName("redelivery-test");

        queue.enqueue("important".getBytes(UTF_8), "test", Map.of());
        Message msg = queue.tryDequeue().get();

        faultDs.startFailing();

        assertThatThrownBy(msg::ack)
          .isInstanceOf(AckUnknownException.class);

        msg.close();
        faultDs.stopFailing();

        Thread.sleep(1500);

        Optional<Message> redelivered = queue.tryDequeue();
        assertThat(redelivered)
          .as("message should be redelivered after visibility timeout when ack failed")
          .isPresent();
        assertThat(redelivered.get().payload()).isEqualTo("important".getBytes(UTF_8));
        assertThat(redelivered.get().attempts()).isEqualTo(2);
        redelivered.get().ack();
    }

    @Test
    void leaderElectionShouldRevokeOnAutoRenewFailure() throws Exception {
        FaultDataSource faultDs = new FaultDataSource(dataSource);

        CountDownLatch elected = new CountDownLatch(1);
        CountDownLatch revoked = new CountDownLatch(1);

        LeaderElection election = Fencepost.Locks.leaderElection(faultDs, "fault-election", Duration.ofSeconds(2))
          .withRenewInterval(Duration.ofMillis(200))
          .withPollInterval(Duration.ofMillis(200))
          .onElected(token -> elected.countDown())
          .onRevoked(revoked::countDown)
          .build();

        election.start();

        assertThat(elected.await(5, TimeUnit.SECONDS))
          .as("should become leader")
          .isTrue();
        assertThat(election.isLeader()).isTrue();

        faultDs.startFailing();

        assertThat(revoked.await(10, TimeUnit.SECONDS))
          .as("leadership should be revoked after auto-renew failure")
          .isTrue();
        assertThat(election.isLeader()).isFalse();

        election.close();
    }

    static final class FaultDataSource implements DataSource {
        private final DataSource delegate;
        private volatile boolean failing;
        private final AtomicInteger remainingFailures = new AtomicInteger(0);

        FaultDataSource(DataSource delegate) {
            this.delegate = delegate;
        }

        void startFailing() {
            failing = true;
        }

        void stopFailing() {
            failing = false;
        }

        void failNext(int n) {
            remainingFailures.set(n);
        }

        @Override
        public Connection getConnection() throws SQLException {
            if (failing || remainingFailures.getAndUpdate(v -> v > 0 ? v - 1 : 0) > 0) {
                throw new SQLException("simulated connection failure");
            }
            return delegate.getConnection();
        }

        @Override
        public Connection getConnection(String u, String p) throws SQLException {
            return getConnection();
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
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return delegate.isWrapperFor(iface);
        }
    }

    private static DataSource failOnSqlContaining(DataSource real, String fragment) {
        return (DataSource) Proxy.newProxyInstance(
          DataSource.class.getClassLoader(),
          new Class<?>[]{DataSource.class},
          (proxy, method, args) -> {
              if ("getConnection".equals(method.getName()) && (args == null || args.length == 0)) {
                  return connectionFailingOnSql(real.getConnection(), fragment);
              }
              try {
                  return method.invoke(real, args);
              } catch (InvocationTargetException e) {
                  throw e.getCause();
              }
          });
    }

    private static Connection connectionFailingOnSql(Connection real, String fragment) {
        return (Connection) Proxy.newProxyInstance(
          Connection.class.getClassLoader(),
          new Class<?>[]{Connection.class},
          (proxy, method, args) -> {
              if ("prepareStatement".equals(method.getName())
                && args != null && args.length > 0 && args[0] instanceof String
                && ((String) args[0]).contains(fragment)) {
                  PreparedStatement ps = real.prepareStatement((String) args[0]);
                  return failingStatement(ps);
              }
              try {
                  return method.invoke(real, args);
              } catch (InvocationTargetException e) {
                  throw e.getCause();
              }
          });
    }

    private static PreparedStatement failingStatement(PreparedStatement real) {
        return (PreparedStatement) Proxy.newProxyInstance(
          PreparedStatement.class.getClassLoader(),
          new Class<?>[]{PreparedStatement.class},
          (proxy, method, args) -> {
              if ("executeQuery".equals(method.getName()) || "executeUpdate".equals(method.getName())) {
                  throw new SQLException("simulated failure during SQL execution", "08006");
              }
              try {
                  return method.invoke(real, args);
              } catch (InvocationTargetException e) {
                  throw e.getCause();
              }
          });
    }

    private static DataSource recordingAutoCommitOnClose(DataSource real, List<Boolean> recorded) {
        return (DataSource) Proxy.newProxyInstance(
          DataSource.class.getClassLoader(),
          new Class<?>[]{DataSource.class},
          (proxy, method, args) -> {
              if ("getConnection".equals(method.getName()) && (args == null || args.length == 0)) {
                  return connectionRecordingAutoCommitOnClose(real.getConnection(), recorded);
              }
              try {
                  return method.invoke(real, args);
              } catch (InvocationTargetException e) {
                  throw e.getCause();
              }
          });
    }

    private static Connection connectionRecordingAutoCommitOnClose(Connection real, List<Boolean> recorded) {
        return (Connection) Proxy.newProxyInstance(
          Connection.class.getClassLoader(),
          new Class<?>[]{Connection.class},
          (proxy, method, args) -> {
              if ("close".equals(method.getName())) {
                  recorded.add(real.getAutoCommit());
              }
              try {
                  return method.invoke(real, args);
              } catch (InvocationTargetException e) {
                  throw e.getCause();
              }
          });
    }

    private static DataSource failOnCommit(DataSource real) {
        return (DataSource) Proxy.newProxyInstance(
          DataSource.class.getClassLoader(),
          new Class<?>[]{DataSource.class},
          (proxy, method, args) -> {
              if ("getConnection".equals(method.getName()) && (args == null || args.length == 0)) {
                  return connectionFailingOnCommit(real.getConnection());
              }
              try {
                  return method.invoke(real, args);
              } catch (InvocationTargetException e) {
                  throw e.getCause();
              }
          });
    }

    private static Connection connectionFailingOnCommit(Connection real) {
        return (Connection) Proxy.newProxyInstance(
          Connection.class.getClassLoader(),
          new Class<?>[]{Connection.class},
          (proxy, method, args) -> {
              if ("commit".equals(method.getName())) {
                  throw new SQLException("simulated commit failure");
              }
              try {
                  return method.invoke(real, args);
              } catch (InvocationTargetException e) {
                  throw e.getCause();
              }
          });
    }
}
