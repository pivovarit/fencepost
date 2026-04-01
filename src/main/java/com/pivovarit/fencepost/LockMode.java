package com.pivovarit.fencepost;

import java.time.Duration;

public interface LockMode {

    static Connection connection() {
        return Connection.INSTANCE;
    }

    static Expiring expiring(Duration lockAtMost) {
        if (lockAtMost.isNegative() || lockAtMost.isZero()) {
            throw new IllegalArgumentException("lockAtMost must be positive");
        }
        return new Expiring(lockAtMost, null, null);
    }

    static Expiring expiring(Duration expiryWindow, Duration refreshInterval) {
        if (expiryWindow.isNegative() || expiryWindow.isZero()) {
            throw new IllegalArgumentException("Expiry window must be positive");
        }
        if (refreshInterval.isNegative() || refreshInterval.isZero()) {
            throw new IllegalArgumentException("Refresh interval must be positive");
        }
        if (refreshInterval.compareTo(expiryWindow) >= 0) {
            throw new IllegalArgumentException("Refresh interval must be less than expiry window");
        }
        return new Expiring(expiryWindow, refreshInterval, null);
    }

    final class Connection implements LockMode {
        private static final Connection INSTANCE = new Connection(null);

        private final Duration keepaliveInterval;

        private Connection(Duration keepaliveInterval) {
            this.keepaliveInterval = keepaliveInterval;
        }

        public Connection withKeepalive(Duration keepaliveInterval) {
            if (keepaliveInterval.isNegative() || keepaliveInterval.isZero()) {
                throw new IllegalArgumentException("keepaliveInterval must be positive");
            }
            return new Connection(keepaliveInterval);
        }

        public Duration keepaliveInterval() {
            return keepaliveInterval;
        }

        public boolean hasKeepalive() {
            return keepaliveInterval != null;
        }
    }

    final class Expiring implements LockMode {
        private final Duration expiryWindow;
        private final Duration refreshInterval;
        private final Duration quietPeriod;

        private Expiring(Duration expiryWindow, Duration refreshInterval, Duration quietPeriod) {
            this.expiryWindow = expiryWindow;
            this.refreshInterval = refreshInterval;
            this.quietPeriod = quietPeriod;
        }

        public Duration expiryWindow() {
            return expiryWindow;
        }

        public Duration refreshInterval() {
            return refreshInterval;
        }

        public Duration quietPeriod() {
            return quietPeriod;
        }

        public boolean hasHeartbeat() {
            return refreshInterval != null;
        }

        public Expiring withQuietPeriod(Duration quietPeriod) {
            if (quietPeriod.isNegative() || quietPeriod.isZero()) {
                throw new IllegalArgumentException("quietPeriod must be positive");
            }
            return new Expiring(this.expiryWindow, this.refreshInterval, quietPeriod);
        }
    }
}
