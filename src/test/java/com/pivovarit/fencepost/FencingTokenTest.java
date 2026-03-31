package com.pivovarit.fencepost;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FencingTokenTest {

    @Test
    void shouldCompareByValue() {
        FencingToken lower = new FencingToken(1);
        FencingToken higher = new FencingToken(2);

        assertThat(lower).isLessThan(higher);
        assertThat(higher).isGreaterThan(lower);
    }

    @Test
    void shouldExposeValue() {
        FencingToken token = new FencingToken(42);
        assertThat(token.value()).isEqualTo(42);
    }

    @Test
    void shouldBeEqualByValue() {
        assertThat(new FencingToken(1)).isEqualTo(new FencingToken(1));
        assertThat(new FencingToken(1)).isNotEqualTo(new FencingToken(2));
    }
}
