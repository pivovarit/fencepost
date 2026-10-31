package com.pivovarit.fencepost;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueueChannelsTest {

    @Test
    void shouldKeepTheChannelNameDerivationStable() {
        assertThat(QueueChannels.name("orders"))
          .isEqualTo("fencepost_q_" + Long.toUnsignedString(HashUtils.fnv1a64("fencepost:orders")));
    }

    @Test
    void shouldGiveDistinctQueuesDistinctChannels() {
        assertThat(QueueChannels.name("orders")).isNotEqualTo(QueueChannels.name("invoices"));
    }

    @Test
    void shouldFitInAPostgresIdentifier() {
        assertThat(QueueChannels.name("x".repeat(49)).length()).isLessThanOrEqualTo(63);
    }
}
