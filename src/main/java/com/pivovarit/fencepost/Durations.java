package com.pivovarit.fencepost;

import java.time.Duration;
import java.util.Objects;

final class Durations {

    private static final Duration ONE_MILLISECOND = Duration.ofMillis(1);

    private Durations() {
    }

    static long toPositiveMillis(Duration duration, String name) {
        requireAtLeastOneMillisecond(duration, name);
        return duration.toMillis();
    }

    static long toNonNegativeMillis(Duration duration, String name) {
        Objects.requireNonNull(duration, name + " must not be null");
        if (duration.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        if (!duration.isZero() && duration.compareTo(ONE_MILLISECOND) < 0) {
            throw new IllegalArgumentException(name + " must be zero or at least 1 millisecond");
        }
        return duration.toMillis();
    }

    static void requireAtLeastOneMillisecond(Duration duration, String name) {
        Objects.requireNonNull(duration, name + " must not be null");
        if (duration.compareTo(ONE_MILLISECOND) < 0) {
            throw new IllegalArgumentException(name + " must be at least 1 millisecond");
        }
    }
}
