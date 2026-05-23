package com.pivovarit.fencepost;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DurationsTest {

    @Test
    void toNonNegativeMillisShouldAcceptZero() {
        long result = Durations.toNonNegativeMillis(Duration.ZERO, "test");

        assertThat(result).isZero();
    }

    @Test
    void toNonNegativeMillisShouldAcceptOneMillisecond() {
        long result = Durations.toNonNegativeMillis(Duration.ofMillis(1), "test");

        assertThat(result).isEqualTo(1);
    }

    @Test
    void toNonNegativeMillisShouldRejectNegativeDuration() {
        assertThatThrownBy(() -> Durations.toNonNegativeMillis(Duration.ofMillis(-1), "timeout"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must not be negative");
    }

    @Test
    void toNonNegativeMillisShouldRejectSubMillisecondPositiveDuration() {
        assertThatThrownBy(() -> Durations.toNonNegativeMillis(Duration.ofNanos(500_000), "timeout"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must be zero or at least 1 millisecond");
    }

    @Test
    void toNonNegativeMillisShouldRejectNull() {
        assertThatThrownBy(() -> Durations.toNonNegativeMillis(null, "timeout"))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("must not be null");
    }

    @Test
    void toPositiveMillisShouldAcceptOneMillisecond() {
        long result = Durations.toPositiveMillis(Duration.ofMillis(1), "test");

        assertThat(result).isEqualTo(1);
    }

    @Test
    void toPositiveMillisShouldRejectZero() {
        assertThatThrownBy(() -> Durations.toPositiveMillis(Duration.ZERO, "timeout"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must be at least 1 millisecond");
    }

    @Test
    void toPositiveMillisShouldRejectNull() {
        assertThatThrownBy(() -> Durations.toPositiveMillis(null, "timeout"))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void requireAtLeastOneMillisecondShouldRejectSubMillisecond() {
        assertThatThrownBy(() -> Durations.requireAtLeastOneMillisecond(Duration.ofNanos(1), "interval"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must be at least 1 millisecond");
    }

    @Test
    void requireAtLeastOneMillisecondShouldAcceptExactlyOneMillisecond() {
        Durations.requireAtLeastOneMillisecond(Duration.ofMillis(1), "interval");
    }

    @Test
    void requireAtLeastOneMillisecondShouldRejectNull() {
        assertThatThrownBy(() -> Durations.requireAtLeastOneMillisecond(null, "interval"))
          .isInstanceOf(NullPointerException.class);
    }
}
