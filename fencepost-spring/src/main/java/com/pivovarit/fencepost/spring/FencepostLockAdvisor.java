package com.pivovarit.fencepost.spring;

import org.aopalliance.aop.Advice;
import org.springframework.aop.Pointcut;
import org.springframework.aop.support.AbstractPointcutAdvisor;
import org.springframework.aop.support.annotation.AnnotationMatchingPointcut;

class FencepostLockAdvisor extends AbstractPointcutAdvisor {

    private final Pointcut pointcut = AnnotationMatchingPointcut.forMethodAnnotation(FencepostLock.class);
    private final FencepostLockInterceptor interceptor;

    FencepostLockAdvisor(FencepostLockInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    @Override
    public Pointcut getPointcut() {
        return pointcut;
    }

    @Override
    public Advice getAdvice() {
        return interceptor;
    }
}
