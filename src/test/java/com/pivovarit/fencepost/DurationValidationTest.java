package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.AdvisoryLock;
import com.pivovarit.fencepost.lock.FencedLock;
import com.pivovarit.fencepost.lock.RenewableLock;
import com.pivovarit.fencepost.queue.Queue;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DurationValidationTest {

    private static final Duration SUB_MILLISECOND = Duration.ofNanos(1);
    private static final DataSource FAILING_DATA_SOURCE = new FailingDataSource();

    @Test
    void sessionLockTimeoutShouldRejectDurationsBelowOneMillisecond() {
        FencedLock lock = Fencepost.Locks.session(FAILING_DATA_SOURCE).build().forName("session-timeout");

        assertThatThrownBy(() -> lock.lock(Duration.ZERO))
          .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> lock.lock(SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void advisoryLockTimeoutShouldRejectDurationsBelowOneMillisecond() {
        AdvisoryLock lock = Fencepost.Locks.advisory(FAILING_DATA_SOURCE).build().forName("advisory-timeout");

        assertThatThrownBy(() -> lock.lock(Duration.ZERO))
          .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> lock.lock(SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void leaseLockDurationsShouldRejectDurationsBelowOneMillisecond() {
        assertThatThrownBy(() -> Fencepost.Locks.lease(FAILING_DATA_SOURCE, SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);

        RenewableLock lock = Fencepost.Locks.lease(FAILING_DATA_SOURCE, Duration.ofMillis(1)).build().forName("lease-timeout");
        assertThatThrownBy(() -> lock.lock(SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> lock.renew(SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void leaseLockBuilderShouldRejectDurationsBelowOneMillisecond() {
        Fencepost.Locks.LeaseBuilder builder = Fencepost.Locks.lease(FAILING_DATA_SOURCE, Duration.ofSeconds(1));

        assertThatThrownBy(() -> builder.withAutoRenew(SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.withQuietPeriod(SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.withPollInterval(SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void leaderElectionBuilderShouldRejectDurationsBelowOneMillisecond() {
        assertThatThrownBy(() -> Fencepost.Locks.leaderElection(FAILING_DATA_SOURCE, "leader", SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);

        Fencepost.Locks.LeaderElectionBuilder builder = Fencepost.Locks.leaderElection(FAILING_DATA_SOURCE, "leader", Duration.ofSeconds(1));
        assertThatThrownBy(() -> builder.withRenewInterval(SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.withQuietPeriod(SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.withPollInterval(SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void queueDurationsShouldRejectDurationsBelowOneMillisecond() {
        assertThatThrownBy(() -> Fencepost.Queues.queue(FAILING_DATA_SOURCE).visibilityTimeout(SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Fencepost.Queues.queue(FAILING_DATA_SOURCE).pollInterval(SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);

        Queue queue = Fencepost.Queues.queue(FAILING_DATA_SOURCE).visibilityTimeout(Duration.ofSeconds(1)).build().forName("queue");
        assertThatThrownBy(() -> queue.enqueue(new byte[0], SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> queue.dequeue(SUB_MILLISECOND))
          .isInstanceOf(IllegalArgumentException.class);
    }

    private static final class FailingDataSource implements DataSource {
        @Override
        public Connection getConnection() {
            throw new AssertionError("Duration validation should happen before opening a connection");
        }

        @Override
        public Connection getConnection(String username, String password) {
            throw new AssertionError("Duration validation should happen before opening a connection");
        }

        @Override
        public PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) {
        }

        @Override
        public void setLoginTimeout(int seconds) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("Not a wrapper");
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }
    }
}
