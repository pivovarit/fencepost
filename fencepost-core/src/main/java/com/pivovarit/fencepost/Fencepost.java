package com.pivovarit.fencepost;

import com.pivovarit.fencepost.lock.AdvisoryLock;
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

        public Factory<RenewableLock> build() {
            return new Factory<>(lockName -> new LeaseLockInstance(lockName, dataSource,
              this.tableName,
              this.leaseDuration,
              this.refreshInterval,
              this.quietPeriod,
              this.pollInterval,
              this.onAutoRenewFailure));
        }
    }

    public static QueueBuilder queue(DataSource dataSource) {
        return new QueueBuilder(Objects.requireNonNull(dataSource, "dataSource must not be null"));
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
