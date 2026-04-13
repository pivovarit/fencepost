package com.pivovarit.fencepost.spring;

import org.junit.jupiter.api.Test;
import java.lang.reflect.Method;
import static org.assertj.core.api.Assertions.assertThat;

class FencepostLockAnnotationTest {

    @Test
    void shouldHaveCorrectDefaults() throws NoSuchMethodException {
        Method method = AnnotatedSample.class.getDeclaredMethod("withDefaults");
        FencepostLock annotation = method.getAnnotation(FencepostLock.class);

        assertThat(annotation.name()).isEqualTo("test-lock");
        assertThat(annotation.lockAtMostFor()).isEqualTo("10m");
        assertThat(annotation.lockAtLeastFor()).isEqualTo("PT0S");
        assertThat(annotation.type()).isEqualTo(LockType.LEASE);
    }

    @Test
    void shouldAllowOverridingAllAttributes() throws NoSuchMethodException {
        Method method = AnnotatedSample.class.getDeclaredMethod("withOverrides");
        FencepostLock annotation = method.getAnnotation(FencepostLock.class);

        assertThat(annotation.name()).isEqualTo("custom-lock");
        assertThat(annotation.lockAtMostFor()).isEqualTo("PT30M");
        assertThat(annotation.lockAtLeastFor()).isEqualTo("5s");
        assertThat(annotation.type()).isEqualTo(LockType.ADVISORY);
    }

    static class AnnotatedSample {
        @FencepostLock(name = "test-lock", lockAtMostFor = "10m")
        void withDefaults() {}

        @FencepostLock(name = "custom-lock", lockAtMostFor = "PT30M", lockAtLeastFor = "5s", type = LockType.ADVISORY)
        void withOverrides() {}
    }
}
