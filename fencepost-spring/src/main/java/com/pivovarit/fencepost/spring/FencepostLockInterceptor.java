package com.pivovarit.fencepost.spring;

import com.pivovarit.fencepost.Factory;
import com.pivovarit.fencepost.Fencepost;
import com.pivovarit.fencepost.lock.AdvisoryLock;
import com.pivovarit.fencepost.lock.FencedLock;
import com.pivovarit.fencepost.lock.FencingToken;
import com.pivovarit.fencepost.lock.RenewableLock;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

class FencepostLockInterceptor implements MethodInterceptor {

    private static final Logger log = LoggerFactory.getLogger(FencepostLockInterceptor.class);

    private final DataSource dataSource;
    private final String tableName;
    private final Duration defaultLockAtMostFor;
    private final UnaryOperator<String> placeholderResolver;

    private volatile Factory<AdvisoryLock> advisoryFactory;
    private volatile Factory<FencedLock> sessionFactory;
    private final ConcurrentHashMap<LeaseConfig, Factory<RenewableLock>> leaseFactories = new ConcurrentHashMap<>();

    FencepostLockInterceptor(DataSource dataSource,
                             String tableName,
                             Duration defaultLockAtMostFor,
                             UnaryOperator<String> placeholderResolver) {
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.defaultLockAtMostFor = defaultLockAtMostFor;
        this.placeholderResolver = placeholderResolver;
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        Method method = invocation.getMethod();
        FencepostLock annotation = method.getAnnotation(FencepostLock.class);

        String lockName = placeholderResolver.apply(annotation.name());
        LockType type = annotation.type();

        return switch (type) {
            case LEASE -> invokeWithLease(invocation, lockName, annotation);
            case SESSION -> invokeWithSession(invocation, lockName);
            case ADVISORY -> invokeWithAdvisory(invocation, lockName);
        };
    }

    private Object invokeWithLease(MethodInvocation invocation, String lockName, FencepostLock annotation) throws Throwable {
        Duration lockAtMostFor = resolveDuration(annotation.lockAtMostFor(), defaultLockAtMostFor);
        Duration lockAtLeastFor = parseDuration(annotation.lockAtLeastFor());

        var config = new LeaseConfig(lockAtMostFor, lockAtLeastFor);
        Factory<RenewableLock> factory = leaseFactories.computeIfAbsent(config, c -> {
            var builder = Fencepost.leaseLock(dataSource, c.lockAtMostFor).tableName(tableName);
            if (c.lockAtLeastFor != null && !c.lockAtLeastFor.isZero()) {
                builder.withQuietPeriod(c.lockAtLeastFor);
            }
            return builder.build();
        });

        RenewableLock lock = factory.forName(lockName);
        Optional<FencingToken> token = lock.tryLock();
        if (token.isEmpty()) {
            log.debug("Skipping execution of {}, lock '{}' is held", invocation.getMethod().getName(), lockName);
            return null;
        }
        try {
            injectFencingToken(invocation, token.get());
            return invocation.proceed();
        } finally {
            lock.unlock();
        }
    }

    private Object invokeWithSession(MethodInvocation invocation, String lockName) throws Throwable {
        if (sessionFactory == null) {
            sessionFactory = Fencepost.sessionLock(dataSource).tableName(tableName).build();
        }
        FencedLock lock = sessionFactory.forName(lockName);
        Optional<FencingToken> token = lock.tryLock();
        if (token.isEmpty()) {
            log.debug("Skipping execution of {}, lock '{}' is held", invocation.getMethod().getName(), lockName);
            return null;
        }
        try {
            injectFencingToken(invocation, token.get());
            return invocation.proceed();
        } finally {
            lock.unlock();
        }
    }

    private Object invokeWithAdvisory(MethodInvocation invocation, String lockName) throws Throwable {
        if (advisoryFactory == null) {
            advisoryFactory = Fencepost.advisoryLock(dataSource).build();
        }
        AdvisoryLock lock = advisoryFactory.forName(lockName);
        if (!lock.tryLock()) {
            log.debug("Skipping execution of {}, lock '{}' is held", invocation.getMethod().getName(), lockName);
            return null;
        }
        try {
            return invocation.proceed();
        } finally {
            lock.unlock();
        }
    }

    private void injectFencingToken(MethodInvocation invocation, FencingToken token) {
        Object[] args = invocation.getArguments();
        Class<?>[] paramTypes = invocation.getMethod().getParameterTypes();
        for (int i = 0; i < paramTypes.length; i++) {
            if (paramTypes[i] == FencingToken.class) {
                args[i] = token;
                return;
            }
        }
    }

    private Duration resolveDuration(String annotationValue, Duration fallback) {
        Duration parsed = parseDuration(annotationValue);
        if (parsed != null && !parsed.isZero()) {
            return parsed;
        }
        return fallback;
    }

    static Duration parseDuration(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Duration.parse(value);
        } catch (Exception e) {
            return DurationParser.parse(value);
        }
    }

    private record LeaseConfig(Duration lockAtMostFor, Duration lockAtLeastFor) {}
}
