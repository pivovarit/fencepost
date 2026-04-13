package com.pivovarit.fencepost.spring;

import com.pivovarit.fencepost.lock.FencingToken;
import org.aopalliance.intercept.MethodInvocation;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Testcontainers
class FencepostLockInterceptorTest {

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:17");

    static DataSource dataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        return ds;
    }

    static void createTable(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute(
                "DROP TABLE IF EXISTS fencepost_locks; " +
                "CREATE TABLE fencepost_locks (" +
                "  lock_name TEXT PRIMARY KEY," +
                "  token BIGINT NOT NULL DEFAULT 0," +
                "  locked_by TEXT," +
                "  locked_at TIMESTAMP WITH TIME ZONE," +
                "  expires_at TIMESTAMP WITH TIME ZONE" +
                ")"
            );
        }
    }

    @Test
    void shouldProceedWhenLeaseLockAcquired() throws Throwable {
        DataSource ds = dataSource();
        createTable(ds);
        var interceptor = new FencepostLockInterceptor(ds, "fencepost_locks", null, name -> name);
        MethodInvocation invocation = mockInvocation("leaseLocked", "expected-result");

        Object result = interceptor.invoke(invocation);

        assertThat(result).isEqualTo("expected-result");
        verify(invocation).proceed();
    }

    @Test
    void shouldInjectFencingTokenParameter() throws Throwable {
        DataSource ds = dataSource();
        createTable(ds);
        var interceptor = new FencepostLockInterceptor(ds, "fencepost_locks", null, name -> name);

        Method method = SampleMethods.class.getDeclaredMethod("leaseLockedWithToken", FencingToken.class);
        MethodInvocation invocation = mock(MethodInvocation.class);
        when(invocation.getMethod()).thenReturn(method);
        when(invocation.getArguments()).thenReturn(new Object[]{null});
        when(invocation.proceed()).thenReturn("ok");

        interceptor.invoke(invocation);

        verify(invocation).proceed();
        assertThat(invocation.getArguments()[0]).isNotNull();
        assertThat(((FencingToken) invocation.getArguments()[0]).value()).isPositive();
    }

    @Test
    void shouldAcquireAdvisoryLockWhenTypeIsAdvisory() throws Throwable {
        DataSource ds = dataSource();
        var interceptor = new FencepostLockInterceptor(ds, "fencepost_locks", null, name -> name);
        MethodInvocation invocation = mockInvocation("advisoryLocked", "result");

        Object result = interceptor.invoke(invocation);

        assertThat(result).isEqualTo("result");
        verify(invocation).proceed();
    }

    @Test
    void shouldAcquireSessionLockWhenTypeIsSession() throws Throwable {
        DataSource ds = dataSource();
        createTable(ds);
        var interceptor = new FencepostLockInterceptor(ds, "fencepost_locks", null, name -> name);
        MethodInvocation invocation = mockInvocation("sessionLocked", "result");

        Object result = interceptor.invoke(invocation);

        assertThat(result).isEqualTo("result");
        verify(invocation).proceed();
    }

    private MethodInvocation mockInvocation(String methodName, Object returnValue) throws Throwable {
        Method method = SampleMethods.class.getDeclaredMethod(methodName);
        MethodInvocation invocation = mock(MethodInvocation.class);
        when(invocation.getMethod()).thenReturn(method);
        when(invocation.getArguments()).thenReturn(new Object[0]);
        when(invocation.proceed()).thenReturn(returnValue);
        return invocation;
    }

    @Test
    void shouldResolvePlaceholdersInLockName() throws Throwable {
        DataSource ds = dataSource();
        createTable(ds);
        var interceptor = new FencepostLockInterceptor(
            ds, "fencepost_locks", null,
            name -> name.replace("${lock.name}", "resolved-lock")
        );

        Method method = SampleMethods.class.getDeclaredMethod("placeholderLocked");
        MethodInvocation invocation = mock(MethodInvocation.class);
        when(invocation.getMethod()).thenReturn(method);
        when(invocation.getArguments()).thenReturn(new Object[0]);
        when(invocation.proceed()).thenReturn("ok");

        Object result = interceptor.invoke(invocation);

        assertThat(result).isEqualTo("ok");
        verify(invocation).proceed();
    }

    @Test
    void shouldThrowWhenLockAtMostForMissingAndNoDefault() throws Throwable {
        DataSource ds = dataSource();
        var interceptor = new FencepostLockInterceptor(ds, "fencepost_locks", null, name -> name);

        Method method = SampleMethods.class.getDeclaredMethod("noLockAtMostFor");
        MethodInvocation invocation = mock(MethodInvocation.class);
        when(invocation.getMethod()).thenReturn(method);
        when(invocation.getArguments()).thenReturn(new Object[0]);

        assertThatThrownBy(() -> interceptor.invoke(invocation))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("lockAtMostFor");
    }

    @Test
    void shouldThrowWhenFencingTokenUsedWithAdvisory() throws Throwable {
        DataSource ds = dataSource();
        var interceptor = new FencepostLockInterceptor(ds, "fencepost_locks", null, name -> name);

        Method method = SampleMethods.class.getDeclaredMethod("advisoryWithToken", FencingToken.class);
        MethodInvocation invocation = mock(MethodInvocation.class);
        when(invocation.getMethod()).thenReturn(method);
        when(invocation.getArguments()).thenReturn(new Object[]{null});

        assertThatThrownBy(() -> interceptor.invoke(invocation))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("FencingToken")
            .hasMessageContaining("ADVISORY");
    }

    static class SampleMethods {
        @FencepostLock(name = "test-lock", lockAtMostFor = "10m")
        Object leaseLocked() { return null; }

        @FencepostLock(name = "test-lock", lockAtMostFor = "10m")
        Object leaseLockedWithToken(FencingToken token) { return null; }

        @FencepostLock(name = "test-lock", lockAtMostFor = "10m", type = LockType.ADVISORY)
        Object advisoryLocked() { return null; }

        @FencepostLock(name = "test-lock", lockAtMostFor = "10m", type = LockType.SESSION)
        Object sessionLocked() { return null; }

        @FencepostLock(name = "${lock.name}", lockAtMostFor = "10m")
        Object placeholderLocked() { return null; }

        @FencepostLock(name = "test-lock")
        Object noLockAtMostFor() { return null; }

        @FencepostLock(name = "test-lock", lockAtMostFor = "10m", type = LockType.ADVISORY)
        Object advisoryWithToken(FencingToken token) { return null; }
    }
}
