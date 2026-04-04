package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public final class Fencepost<T extends FencepostLock> {

    private final Function<String, T> lockFactory;

    private Fencepost(Function<String, T> lockFactory) {
        this.lockFactory = lockFactory;
    }

    public T forName(String lockName) {
        Objects.requireNonNull(lockName, "lockName must not be null");
        return lockFactory.apply(lockName);
    }

    public static AdvisoryBuilder advisoryLock(DataSource dataSource) {
        return new AdvisoryBuilder(Objects.requireNonNull(dataSource, "dataSource must not be null"));
    }

    public static ConnectionBuilder connectionLock(DataSource dataSource) {
        return new ConnectionBuilder(Objects.requireNonNull(dataSource, "dataSource must not be null"));
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

        public Fencepost<FencepostLock> build() {
            return new Fencepost<>(lockName -> new AdvisoryLockInstance(lockName, dataSource));
        }
    }

    public static final class ConnectionBuilder {
        private final DataSource dataSource;
        private String tableName = "fencepost_locks";

        private ConnectionBuilder(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public ConnectionBuilder tableName(String tableName) {
            Objects.requireNonNull(tableName);
            if (!tableName.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                throw new IllegalArgumentException("Invalid table name: " + tableName);
            }
            this.tableName = tableName;
            return this;
        }

        public Fencepost<FencedLock> build() {
            String t = this.tableName;
            return new Fencepost<>(lockName -> new ConnectionLockInstance(lockName, dataSource, t));
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

        public Fencepost<RenewableLock> build() {
            return new Fencepost<>(lockName -> new LeaseLockInstance(lockName, dataSource,
              this.tableName,
              this.leaseDuration,
              this.refreshInterval,
              this.quietPeriod,
              this.onHeartbeatFailure));
        }
    }
}
