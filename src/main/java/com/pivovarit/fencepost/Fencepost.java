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

    public static AdvisoryBuilder advisory(DataSource dataSource) {
        return new AdvisoryBuilder(Objects.requireNonNull(dataSource, "dataSource must not be null"));
    }

    public static ConnectionBuilder connection(DataSource dataSource) {
        return new ConnectionBuilder(Objects.requireNonNull(dataSource, "dataSource must not be null"));
    }

    public static ExpiringBuilder expiring(DataSource dataSource, Duration lockAtMost) {
        Objects.requireNonNull(dataSource, "dataSource must not be null");
        if (lockAtMost.isNegative() || lockAtMost.isZero()) {
            throw new IllegalArgumentException("lockAtMost must be positive");
        }
        return new ExpiringBuilder(dataSource, lockAtMost);
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

    public static final class ExpiringBuilder {
        private final DataSource dataSource;
        private final Duration expiryWindow;
        private String tableName = "fencepost_locks";
        private Duration refreshInterval;
        private Duration quietPeriod;
        private Consumer<FencepostException> onHeartbeatFailure;

        private ExpiringBuilder(DataSource dataSource, Duration expiryWindow) {
            this.dataSource = dataSource;
            this.expiryWindow = expiryWindow;
        }

        public ExpiringBuilder tableName(String tableName) {
            Objects.requireNonNull(tableName);
            if (!tableName.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                throw new IllegalArgumentException("Invalid table name: " + tableName);
            }
            this.tableName = tableName;
            return this;
        }

        public ExpiringBuilder withHeartbeat(Duration refreshInterval) {
            if (refreshInterval.isNegative() || refreshInterval.isZero()) {
                throw new IllegalArgumentException("Refresh interval must be positive");
            }
            if (refreshInterval.compareTo(expiryWindow) >= 0) {
                throw new IllegalArgumentException("Refresh interval must be less than expiry window");
            }
            this.refreshInterval = refreshInterval;
            return this;
        }

        public ExpiringBuilder withQuietPeriod(Duration quietPeriod) {
            if (quietPeriod.isNegative() || quietPeriod.isZero()) {
                throw new IllegalArgumentException("quietPeriod must be positive");
            }
            this.quietPeriod = quietPeriod;
            return this;
        }

        public ExpiringBuilder onHeartbeatFailure(Consumer<FencepostException> handler) {
            this.onHeartbeatFailure = Objects.requireNonNull(handler);
            return this;
        }

        public Fencepost<RenewableLock> build() {
            String t = this.tableName;
            Duration ew = this.expiryWindow;
            Duration ri = this.refreshInterval;
            Duration qp = this.quietPeriod;
            Consumer<FencepostException> ohf = this.onHeartbeatFailure;
            return new Fencepost<>(lockName -> new ExpiringLockInstance(lockName, dataSource, t, ew, ri, qp, ohf));
        }
    }
}
