package com.pivovarit.fencepost;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Guards the leader-election loss-detection invariant: a hung database must be
 * detected (auto-renew failure reported) strictly before the lease can expire,
 * otherwise a standby can acquire the lease while {@code isLeader()} still
 * returns true on the old leader (split-brain window).
 */
class LeaseLossDetectionTest {

    @Test
    void worstCaseDetectionShouldBeatLeaseForTheReportedConfig() {
        // lease=3s / renew=1s previously detected loss at ~t0+4.3s (3 attempts floored
        // to a 1s query timeout + backoff) while the lease expired at t0+3s.
        long lease = 3_000;
        long refresh = 1_000;

        assertThat(LeaseLockInstance.worstCaseAutoRenewDetectionMillis(lease, refresh))
          .as("worst-case detection must complete before the lease expires")
          .isLessThan(lease);
    }

    @Test
    void worstCaseDetectionShouldBeatLeaseAcrossDefaultRenewIntervals() {
        for (long lease = 1_000; lease <= 60_000; lease += 1_000) {
            long refresh = lease / 3; // the library default renewInterval
            assertThat(LeaseLockInstance.worstCaseAutoRenewDetectionMillis(lease, refresh))
              .as("lease=%dms refresh=%dms", lease, refresh)
              .isLessThan(lease);
        }
    }

    @Test
    void validationShouldAcceptTheReportedConfig() {
        LeaseLockInstance.validateAutoRenewDetectionBudget(3_000, 1_000);
    }

    @Test
    void validationShouldRejectConfigThatCannotDetectLossBeforeExpiry() {
        // refresh < lease (so withAutoRenew accepts it) but there is no room left to
        // run the renew retries before the lease expires.
        assertThatThrownBy(() -> LeaseLockInstance.validateAutoRenewDetectionBudget(400, 350))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("detect");
    }
}
