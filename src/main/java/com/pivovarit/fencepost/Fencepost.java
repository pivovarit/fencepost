package com.pivovarit.fencepost;

import javax.sql.DataSource;
import java.util.Objects;
import java.util.function.Consumer;

public final class Fencepost {

    private final DataSource dataSource;
    private final String tableName;
    private final LockMode lockMode;
    private final Consumer<FencepostException> onHeartbeatFailure;

    private Fencepost(DataSource dataSource, String tableName, LockMode lockMode, Consumer<FencepostException> onHeartbeatFailure) {
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.lockMode = lockMode;
        this.onHeartbeatFailure = onHeartbeatFailure;
    }

    public FencepostLock forName(String lockName) {
        Objects.requireNonNull(lockName, "lockName must not be null");
        return new FencepostLockInstance(lockName, dataSource, lockMode, tableName, onHeartbeatFailure);
    }

    public static Builder builder(DataSource dataSource) {
        return new Builder(Objects.requireNonNull(dataSource, "dataSource must not be null"));
    }

    public static final class Builder {

        private final DataSource dataSource;
        private String tableName = "fencepost_locks";
        private LockMode lockMode = LockMode.connection();
        private Consumer<FencepostException> onHeartbeatFailure;

        private Builder(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public Builder tableName(String tableName) {
            Objects.requireNonNull(tableName);
            if (!tableName.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                throw new IllegalArgumentException("Invalid table name: " + tableName);
            }
            this.tableName = tableName;
            return this;
        }

        public Builder lockMode(LockMode lockMode) {
            this.lockMode = Objects.requireNonNull(lockMode);
            return this;
        }

        public Builder onHeartbeatFailure(Consumer<FencepostException> handler) {
            this.onHeartbeatFailure = Objects.requireNonNull(handler);
            return this;
        }

        public Fencepost build() {
            return new Fencepost(dataSource, tableName, lockMode, onHeartbeatFailure);
        }
    }
}
