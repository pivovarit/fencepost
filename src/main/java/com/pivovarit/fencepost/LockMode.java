package com.pivovarit.fencepost;

import java.time.Duration;

public interface LockMode {

    static LockMode connection() {
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
        private static final Connection INSTANCE = new Connection();

        private Connection() {
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
