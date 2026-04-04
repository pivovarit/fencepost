package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

public final class Fencepost {

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
            if (!tableName.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
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
        private Consumer<FencepostException> onHeartbeatFailure;

        private LeaseBuilder(DataSource dataSource, Duration leaseDuration) {
            this.dataSource = dataSource;
            this.leaseDuration = leaseDuration;
        }

        public LeaseBuilder tableName(String tableName) {
            Objects.requireNonNull(tableName);
            if (!tableName.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                throw new IllegalArgumentException("Invalid table name: " + tableName);
            }
            this.tableName = tableName;
            return this;
        }

        public LeaseBuilder withHeartbeat(Duration refreshInterval) {
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

        public LeaseBuilder onHeartbeatFailure(Consumer<FencepostException> handler) {
            this.onHeartbeatFailure = Objects.requireNonNull(handler);
            return this;
        }

        public Factory<RenewableLock> build() {
            return new Factory<>(lockName -> new LeaseLockInstance(lockName, dataSource,
              this.tableName,
              this.leaseDuration,
              this.refreshInterval,
              this.quietPeriod,
              this.onHeartbeatFailure));
        }
    }
}
