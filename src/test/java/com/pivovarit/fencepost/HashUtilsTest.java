package com.pivovarit.fencepost;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HashUtilsTest {

    @Test
    void shouldProduceConsistentHash() {
        long hash1 = HashUtils.fnv1a64("test-key");
        long hash2 = HashUtils.fnv1a64("test-key");

        assertThat(hash1).isEqualTo(hash2);
    }

    @Test
    void shouldProduceDifferentHashesForDifferentInputs() {
        long hash1 = HashUtils.fnv1a64("lock-a");
        long hash2 = HashUtils.fnv1a64("lock-b");

        assertThat(hash1).isNotEqualTo(hash2);
    }

    @Test
    void shouldHandleEmptyString() {
        long hash = HashUtils.fnv1a64("");

        assertThat(hash).isEqualTo(0xcbf29ce484222325L);
    }

    @Test
    void shouldHandleSingleCharacter() {
        long hash = HashUtils.fnv1a64("a");

        assertThat(hash).isNotEqualTo(0xcbf29ce484222325L);
    }

    @Test
    void shouldHandleUnicodeCharacters() {
        long hash1 = HashUtils.fnv1a64("zażółć");
        long hash2 = HashUtils.fnv1a64("zażółć");

        assertThat(hash1).isEqualTo(hash2);
    }

    @Test
    void shouldProduceDifferentHashesForSimilarStrings() {
        long hash1 = HashUtils.fnv1a64("ab");
        long hash2 = HashUtils.fnv1a64("ba");

        assertThat(hash1).isNotEqualTo(hash2);
    }

    @Test
    void shouldMatchKnownFnv1a64Value() {
        // FNV-1a 64-bit reference: hash of empty string is the offset basis
        assertThat(HashUtils.fnv1a64("")).isEqualTo(-3750763034362895579L);
    }
}
