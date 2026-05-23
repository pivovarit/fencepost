package com.pivovarit.fencepost;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BuilderValidationTest {

    private static final DataSource FAILING_DATA_SOURCE = new FailingDataSource();

    @Test
    void advisoryBuilderShouldRejectNullDataSource() {
        assertThatThrownBy(() -> Fencepost.Locks.advisory(null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void sessionBuilderShouldRejectNullDataSource() {
        assertThatThrownBy(() -> Fencepost.Locks.session(null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void leaseBuilderShouldRejectNullDataSource() {
        assertThatThrownBy(() -> Fencepost.Locks.lease(null, Duration.ofSeconds(1)))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void leaderElectionBuilderShouldRejectNullDataSource() {
        assertThatThrownBy(() -> Fencepost.Locks.leaderElection(null, "name", Duration.ofSeconds(1)))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void leaderElectionBuilderShouldRejectNullElectionName() {
        assertThatThrownBy(() -> Fencepost.Locks.leaderElection(FAILING_DATA_SOURCE, null, Duration.ofSeconds(1)))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void leaderElectionBuilderShouldRejectEmptyElectionName() {
        assertThatThrownBy(() -> Fencepost.Locks.leaderElection(FAILING_DATA_SOURCE, "", Duration.ofSeconds(1)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must not be empty");
    }

    @Test
    void leaderElectionBuilderShouldRejectNullLeaseDuration() {
        assertThatThrownBy(() -> Fencepost.Locks.leaderElection(FAILING_DATA_SOURCE, "name", null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void leaseAutoRenewShouldRejectIntervalEqualToLeaseDuration() {
        Fencepost.Locks.LeaseBuilder builder = Fencepost.Locks.lease(FAILING_DATA_SOURCE, Duration.ofSeconds(5));

        assertThatThrownBy(() -> builder.withAutoRenew(Duration.ofSeconds(5)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("less than lease duration");
    }

    @Test
    void leaseAutoRenewShouldRejectIntervalGreaterThanLeaseDuration() {
        Fencepost.Locks.LeaseBuilder builder = Fencepost.Locks.lease(FAILING_DATA_SOURCE, Duration.ofSeconds(5));

        assertThatThrownBy(() -> builder.withAutoRenew(Duration.ofSeconds(10)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("less than lease duration");
    }

    @Test
    void leaderElectionRenewShouldRejectIntervalEqualToLeaseDuration() {
        Fencepost.Locks.LeaderElectionBuilder builder =
          Fencepost.Locks.leaderElection(FAILING_DATA_SOURCE, "name", Duration.ofSeconds(5));

        assertThatThrownBy(() -> builder.withRenewInterval(Duration.ofSeconds(5)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("less than leaseDuration");
    }

    @Test
    void leaderElectionRenewShouldRejectIntervalGreaterThanLeaseDuration() {
        Fencepost.Locks.LeaderElectionBuilder builder =
          Fencepost.Locks.leaderElection(FAILING_DATA_SOURCE, "name", Duration.ofSeconds(5));

        assertThatThrownBy(() -> builder.withRenewInterval(Duration.ofSeconds(10)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("less than leaseDuration");
    }

    @Test
    void leaderElectionBuildShouldRejectTinyLeaseDurationCausingZeroDefaults() {
        assertThatThrownBy(() -> Fencepost.Locks.leaderElection(FAILING_DATA_SOURCE, "name", Duration.ofMillis(2)).build())
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void leaseInstanceIdShouldRejectNull() {
        Fencepost.Locks.LeaseBuilder builder = Fencepost.Locks.lease(FAILING_DATA_SOURCE, Duration.ofSeconds(1));

        assertThatThrownBy(() -> builder.withInstanceId(null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void leaseInstanceIdShouldRejectEmpty() {
        Fencepost.Locks.LeaseBuilder builder = Fencepost.Locks.lease(FAILING_DATA_SOURCE, Duration.ofSeconds(1));

        assertThatThrownBy(() -> builder.withInstanceId(""))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must not be empty");
    }

    @Test
    void leaderElectionInstanceIdShouldRejectNull() {
        Fencepost.Locks.LeaderElectionBuilder builder =
          Fencepost.Locks.leaderElection(FAILING_DATA_SOURCE, "name", Duration.ofSeconds(1));

        assertThatThrownBy(() -> builder.withInstanceId(null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void leaderElectionInstanceIdShouldRejectEmpty() {
        Fencepost.Locks.LeaderElectionBuilder builder =
          Fencepost.Locks.leaderElection(FAILING_DATA_SOURCE, "name", Duration.ofSeconds(1));

        assertThatThrownBy(() -> builder.withInstanceId(""))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must not be empty");
    }

    @Test
    void consumerBuilderShouldRejectNullQueueName() {
        assertThatThrownBy(() -> Fencepost.Queues.consumer(FAILING_DATA_SOURCE, null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void consumerBuilderShouldRejectEmptyQueueName() {
        assertThatThrownBy(() -> Fencepost.Queues.consumer(FAILING_DATA_SOURCE, ""))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must not be empty");
    }

    @Test
    void consumerBuilderShouldRejectNegativeConcurrency() {
        assertThatThrownBy(() -> Fencepost.Queues.consumer(FAILING_DATA_SOURCE, "q").concurrency(-1))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("at least 1");
    }

    @Test
    void queueBuilderShouldRejectNullDataSource() {
        assertThatThrownBy(() -> Fencepost.Queues.queue(null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void publisherBuilderShouldRejectNullDataSource() {
        assertThatThrownBy(() -> Fencepost.Queues.publisher(null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void queueBuildShouldRejectMissingVisibilityTimeout() {
        assertThatThrownBy(() -> Fencepost.Queues.queue(FAILING_DATA_SOURCE).build())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("visibilityTimeout must be set");
    }

    @Test
    void consumerBuildShouldRejectMissingVisibilityTimeout() {
        assertThatThrownBy(() -> Fencepost.Queues.consumer(FAILING_DATA_SOURCE, "q").handler(m -> {}).build())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("visibilityTimeout must be set");
    }

    @Test
    void consumerBuildShouldRejectMissingHandler() {
        assertThatThrownBy(() -> Fencepost.Queues.consumer(FAILING_DATA_SOURCE, "q")
          .visibilityTimeout(Duration.ofSeconds(30)).build())
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void leaderElectionShouldRejectNullOnElectedCallback() {
        Fencepost.Locks.LeaderElectionBuilder builder =
          Fencepost.Locks.leaderElection(FAILING_DATA_SOURCE, "name", Duration.ofSeconds(1));

        assertThatThrownBy(() -> builder.onElected((Runnable) null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void leaderElectionShouldRejectNullOnRevokedCallback() {
        Fencepost.Locks.LeaderElectionBuilder builder =
          Fencepost.Locks.leaderElection(FAILING_DATA_SOURCE, "name", Duration.ofSeconds(1));

        assertThatThrownBy(() -> builder.onRevoked(null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void leaderElectionShouldRejectNullSchemaMode() {
        Fencepost.Locks.LeaderElectionBuilder builder =
          Fencepost.Locks.leaderElection(FAILING_DATA_SOURCE, "name", Duration.ofSeconds(1));

        assertThatThrownBy(() -> builder.schemaMode(null))
          .isInstanceOf(NullPointerException.class);
    }

    private static final class FailingDataSource implements DataSource {
        @Override
        public Connection getConnection() {
            throw new AssertionError("Validation should happen before opening a connection");
        }

        @Override
        public Connection getConnection(String username, String password) {
            throw new AssertionError("Validation should happen before opening a connection");
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
