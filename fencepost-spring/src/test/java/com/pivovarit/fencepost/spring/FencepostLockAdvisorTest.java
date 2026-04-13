package com.pivovarit.fencepost.spring;

import org.junit.jupiter.api.Test;
import java.lang.reflect.Method;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class FencepostLockAdvisorTest {

    @Test
    void shouldMatchAnnotatedMethods() throws NoSuchMethodException {
        var advisor = new FencepostLockAdvisor(mock(FencepostLockInterceptor.class));

        Method annotated = Sample.class.getDeclaredMethod("locked");
        Method notAnnotated = Sample.class.getDeclaredMethod("notLocked");

        assertThat(advisor.getPointcut().getMethodMatcher().matches(annotated, Sample.class)).isTrue();
        assertThat(advisor.getPointcut().getMethodMatcher().matches(notAnnotated, Sample.class)).isFalse();
    }

    static class Sample {
        @FencepostLock(name = "test", lockAtMostFor = "10m")
        void locked() {}
        void notLocked() {}
    }
}
