package com.pivovarit.fencepost;

import com.pivovarit.fencepost.election.LeaderElection;
import com.pivovarit.fencepost.lock.AdvisoryLock;
import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.FencedLock;
import com.pivovarit.fencepost.lock.RenewableLock;
import com.pivovarit.fencepost.queue.Queue;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public final class Fencepost {

    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*(\\.[a-zA-Z_][a-zA-Z0-9_]*)*");

    private Fencepost() {
    }

    public static AdvisoryBuilder advisoryLock(DataSource dataSource) {
        return new AdvisoryBuilder(Objects.requireNonNull(dataSource, "dataSource must not be null"));
    }

    public static SessionBuilder sessionLock(DataSource dataSource) {
        return new SessionBuilder(Objects.requireNonNull(dataSource, "dataSource must not be null"));
    }

    public static LeaseBuilder leaseLock(DataSource dataSource, Duration lockAtMost) {
        Objects.requireNonNull(dataSource, "dataSource must not be null");
        if (lockAtMost.isNegative() || lockAtMost.isZero()) {
            throw new IllegalArgumentException("lockAtMost must be positive");
        }
        return new LeaseBuilder(dataSource, lockAtMost);
    }

    public static final class AdvisoryBuilder {
        private final DataSource dataSource;

        private AdvisoryBuilder(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public Factory<AdvisoryLock> build() {
            return new Factory<>(lockName -> new AdvisoryLockInstance(lockName, dataSource));
        }
    }

    public static final class SessionBuilder {
        private final DataSource dataSource;
        private String tableName = "fencepost_locks";

        private SessionBuilder(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public SessionBuilder tableName(String tableName) {
            Objects.requireNonNull(tableName);
            if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
                throw new IllegalArgumentException("Invalid table name: " + tableName);
            }
            this.tableName = tableName;
            return this;
        }

        public Factory<FencedLock> build() {
            String t = this.tableName;
            return new Factory<>(lockName -> new SessionLockInstance(lockName, dataSource, t));
        }
    }

    public static final class LeaseBuilder {
        private final DataSource dataSource;
        private final Duration leaseDuration;
        private String tableName = "fencepost_locks";
        private Duration refreshInterval;
        private Duration quietPeriod;
        private Duration pollInterval;
        private Consumer<FencepostException> onAutoRenewFailure;
        private String instanceId;

        private LeaseBuilder(DataSource dataSource, Duration leaseDuration) {
            this.dataSource = dataSource;
            this.leaseDuration = leaseDuration;
        }

        public LeaseBuilder tableName(String tableName) {
            Objects.requireNonNull(tableName);
            if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
                throw new IllegalArgumentException("Invalid table name: " + tableName);
            }
            this.tableName = tableName;
            return this;
        }

        public LeaseBuilder withAutoRenew(Duration refreshInterval) {
            if (refreshInterval.isNegative() || refreshInterval.isZero()) {
                throw new IllegalArgumentException("Refresh interval must be positive");
            }
            if (refreshInterval.compareTo(leaseDuration) >= 0) {
                throw new IllegalArgumentException("Refresh interval must be less than lease duration");
            }
            this.refreshInterval = refreshInterval;
            return this;
        }

        public LeaseBuilder withQuietPeriod(Duration quietPeriod) {
            if (quietPeriod.isNegative() || quietPeriod.isZero()) {
                throw new IllegalArgumentException("quietPeriod must be positive");
            }
            this.quietPeriod = quietPeriod;
            return this;
        }

        public LeaseBuilder withPollInterval(Duration pollInterval) {
            if (pollInterval.isNegative() || pollInterval.isZero()) {
                throw new IllegalArgumentException("Poll interval must be positive");
            }
            this.pollInterval = pollInterval;
            return this;
        }

        public LeaseBuilder onAutoRenewFailure(Consumer<FencepostException> handler) {
            this.onAutoRenewFailure = Objects.requireNonNull(handler);
            return this;
        }

        public LeaseBuilder withInstanceId(String instanceId) {
            Objects.requireNonNull(instanceId, "instanceId must not be null");
            if (instanceId.isEmpty()) {
                throw new IllegalArgumentException("instanceId must not be empty");
            }
            this.instanceId = instanceId;
            return this;
        }

        public Factory<RenewableLock> build() {
            return new Factory<>(lockName -> new LeaseLockInstance(lockName, dataSource,
              this.tableName,
              this.leaseDuration,
              this.refreshInterval,
              this.quietPeriod,
              this.pollInterval,
              this.onAutoRenewFailure,
              this.instanceId));
        }
    }

    public static LeaderElectionBuilder leaderElection(DataSource dataSource, String electionName, Duration leaseDuration) {
        Objects.requireNonNull(dataSource, "dataSource must not be null");
        Objects.requireNonNull(electionName, "electionName must not be null");
        if (electionName.isEmpty()) {
            throw new IllegalArgumentException("electionName must not be empty");
        }
        Objects.requireNonNull(leaseDuration, "leaseDuration must not be null");
        if (leaseDuration.isNegative() || leaseDuration.isZero()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        return new LeaderElectionBuilder(dataSource, electionName, leaseDuration);
    }

    public static QueueBuilder queue(DataSource dataSource) {
        return new QueueBuilder(Objects.requireNonNull(dataSource, "dataSource must not be null"));
    }

    public static final class LeaderElectionBuilder {
        private final DataSource dataSource;
        private final String electionName;
        private final Duration leaseDuration;
        private String tableName = "fencepost_locks";
        private Duration renewInterval;
        private Duration pollInterval;
        private Duration quietPeriod;
        private String instanceId;
        private Consumer<FencingToken> onElected = token -> {};
        private Runnable onRevoked = () -> {};
        private Consumer<Throwable> onCallbackError = t -> {};

        private LeaderElectionBuilder(DataSource dataSource, String electionName, Duration leaseDuration) {
            this.dataSource = dataSource;
            this.electionName = electionName;
            this.leaseDuration = leaseDuration;
        }

        public LeaderElectionBuilder tableName(String tableName) {
            Objects.requireNonNull(tableName);
            if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
                throw new IllegalArgumentException("Invalid table name: " + tableName);
            }
            this.tableName = tableName;
            return this;
        }

        public LeaderElectionBuilder withRenewInterval(Duration renewInterval) {
            Objects.requireNonNull(renewInterval, "renewInterval must not be null");
            if (renewInterval.isNegative() || renewInterval.isZero()) {
                throw new IllegalArgumentException("renewInterval must be positive");
            }
            if (renewInterval.compareTo(leaseDuration) >= 0) {
                throw new IllegalArgumentException("renewInterval must be less than leaseDuration");
            }
            this.renewInterval = renewInterval;
            return this;
        }

        public LeaderElectionBuilder withPollInterval(Duration pollInterval) {
            Objects.requireNonNull(pollInterval, "pollInterval must not be null");
            if (pollInterval.isNegative() || pollInterval.isZero()) {
                throw new IllegalArgumentException("pollInterval must be positive");
            }
            this.pollInterval = pollInterval;
            return this;
        }

        public LeaderElectionBuilder withQuietPeriod(Duration quietPeriod) {
            Objects.requireNonNull(quietPeriod, "quietPeriod must not be null");
            if (quietPeriod.isNegative() || quietPeriod.isZero()) {
                throw new IllegalArgumentException("quietPeriod must be positive");
            }
            this.quietPeriod = quietPeriod;
            return this;
        }

        public LeaderElectionBuilder withInstanceId(String instanceId) {
            Objects.requireNonNull(instanceId, "instanceId must not be null");
            if (instanceId.isEmpty()) {
                throw new IllegalArgumentException("instanceId must not be empty");
            }
            this.instanceId = instanceId;
            return this;
        }

        public LeaderElectionBuilder onElected(Runnable callback) {
            Objects.requireNonNull(callback, "callback must not be null");
            this.onElected = token -> callback.run();
            return this;
        }

        public LeaderElectionBuilder onElected(Consumer<FencingToken> callback) {
            this.onElected = Objects.requireNonNull(callback, "callback must not be null");
            return this;
        }

        public LeaderElectionBuilder onRevoked(Runnable callback) {
            this.onRevoked = Objects.requireNonNull(callback, "callback must not be null");
            return this;
        }

        public LeaderElectionBuilder onCallbackError(Consumer<Throwable> handler) {
            this.onCallbackError = Objects.requireNonNull(handler, "handler must not be null");
            return this;
        }

        public LeaderElection build() {
            Duration effectiveRenew = renewInterval != null ? renewInterval : leaseDuration.dividedBy(3);
            Duration effectivePoll = pollInterval != null ? pollInterval : leaseDuration.dividedBy(2);
            if (effectiveRenew.isZero()) {
                throw new IllegalArgumentException("Default renewInterval (leaseDuration/3) is zero — leaseDuration is too small");
            }
            if (effectivePoll.isZero()) {
                throw new IllegalArgumentException("Default pollInterval (leaseDuration/2) is zero — leaseDuration is too small");
            }
            return new LeaderElectionInstance(
                electionName,
                dataSource,
                tableName,
                leaseDuration,
                effectiveRenew,
                effectivePoll,
                quietPeriod,
                instanceId,
                onElected,
                onRevoked,
                onCallbackError);
        }
    }

    public static final class QueueBuilder {
        private final DataSource dataSource;
        private String tableName = "fencepost_queue";
        private Duration visibilityTimeout;
        private long pollIntervalMs = 100;

        private QueueBuilder(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public QueueBuilder tableName(String tableName) {
            Objects.requireNonNull(tableName);
            if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
                throw new IllegalArgumentException("Invalid table name: " + tableName);
            }
            this.tableName = tableName;
            return this;
        }

        public QueueBuilder visibilityTimeout(Duration visibilityTimeout) {
            if (visibilityTimeout.isNegative() || visibilityTimeout.isZero()) {
                throw new IllegalArgumentException("visibilityTimeout must be positive");
            }
            this.visibilityTimeout = visibilityTimeout;
            return this;
        }

        public QueueBuilder pollInterval(Duration pollInterval) {
            if (pollInterval.isNegative() || pollInterval.isZero()) {
                throw new IllegalArgumentException("pollInterval must be positive");
            }
            this.pollIntervalMs = pollInterval.toMillis();
            return this;
        }

        public Factory<Queue> build() {
            if (visibilityTimeout == null) {
                throw new IllegalStateException("visibilityTimeout must be set");
            }
            String t = this.tableName;
            Duration vt = this.visibilityTimeout;
            long pi = this.pollIntervalMs;
            return new Factory<>(queueName -> new FencepostQueue(queueName, dataSource, t, vt, pi));
        }
    }
}
