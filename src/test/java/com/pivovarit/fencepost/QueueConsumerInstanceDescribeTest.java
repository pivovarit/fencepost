package com.pivovarit.fencepost;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class QueueConsumerInstanceDescribeTest {

    @Test
    void shouldNotThrowWhenThrowableToStringReturnsNull() {
        Throwable t = new RuntimeException() {
            @Override
            public String toString() {
                return null;
            }
        };

        assertThatCode(() -> QueueConsumerInstance.describe(t)).doesNotThrowAnyException();
        assertThat(QueueConsumerInstance.describe(t)).isNotNull();
    }

    @Test
    void shouldNotSplitSurrogatePairWhenTruncating() {
        // 999 plain chars, then a surrogate pair (😀) straddling index 999/1000,
        // then filler to push length past the 1000-char truncation boundary.
        String message = "a".repeat(999) + "😀" + "b".repeat(100);
        Throwable t = new RuntimeException(message) {
            @Override
            public String toString() {
                return getMessage();
            }
        };

        String result = QueueConsumerInstance.describe(t);

        assertThat(isValidUtf16(result))
          .as("describe() must not leave an unpaired surrogate that PostgreSQL would reject")
          .isTrue();
    }

    // A string with an unpaired surrogate is not representable in UTF-8: encoding
    // replaces it with '?', so a round-trip through UTF-8 no longer matches.
    private static boolean isValidUtf16(String s) {
        return s.equals(new String(s.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8));
    }
}
