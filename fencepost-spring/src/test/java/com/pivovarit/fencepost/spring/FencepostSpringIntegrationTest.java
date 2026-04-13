package com.pivovarit.fencepost.spring;

import com.pivovarit.fencepost.lock.FencingToken;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.aop.framework.autoproxy.DefaultAdvisorAutoProxyCreator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.util.AopTestUtils;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(classes = FencepostSpringIntegrationTest.TestConfig.class)
class FencepostSpringIntegrationTest {

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:17");

    @Autowired
    private LockedService lockedService;

    private LockedService target;

    @BeforeAll
    static void createTable() throws SQLException {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS fencepost_locks (" +
                "  lock_name TEXT PRIMARY KEY," +
                "  token BIGINT NOT NULL DEFAULT 0," +
                "  locked_by TEXT," +
                "  locked_at TIMESTAMP WITH TIME ZONE," +
                "  expires_at TIMESTAMP WITH TIME ZONE" +
                ")"
            );
        }
    }

    @BeforeEach
    void setUp() {
        target = AopTestUtils.getTargetObject(lockedService);
        target.counter.set(0);
        target.lastToken.set(null);
    }

    @Test
    void shouldExecuteWhenLockAvailable() {
        lockedService.leaseLockedMethod();
        assertThat(target.counter.get()).isEqualTo(1);
    }

    @Test
    void shouldInjectFencingToken() {
        lockedService.leaseLockedWithToken(null);
        assertThat(target.counter.get()).isEqualTo(1);
        assertThat(target.lastToken.get()).isNotNull();
        assertThat(target.lastToken.get().value()).isPositive();
    }

    @Test
    void shouldExecuteWithAdvisoryLock() {
        lockedService.advisoryLockedMethod();
        assertThat(target.counter.get()).isEqualTo(1);
    }

    @Test
    void shouldExecuteWithSessionLock() {
        lockedService.sessionLockedMethod();
        assertThat(target.counter.get()).isEqualTo(1);
    }

    @Configuration
    @EnableAutoConfiguration
    static class TestConfig {
        @Bean
        static DefaultAdvisorAutoProxyCreator defaultAdvisorAutoProxyCreator() {
            return new DefaultAdvisorAutoProxyCreator();
        }

        @Bean
        DataSource dataSource() {
            PGSimpleDataSource ds = new PGSimpleDataSource();
            ds.setUrl(PG.getJdbcUrl());
            ds.setUser(PG.getUsername());
            ds.setPassword(PG.getPassword());
            return ds;
        }

        @Bean
        LockedService lockedService() {
            return new LockedService();
        }
    }

    static class LockedService {
        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicReference<FencingToken> lastToken = new AtomicReference<>();

        @FencepostLock(name = "integration-lease", lockAtMostFor = "1m")
        void leaseLockedMethod() {
            counter.incrementAndGet();
        }

        @FencepostLock(name = "integration-lease-token", lockAtMostFor = "1m")
        void leaseLockedWithToken(FencingToken token) {
            counter.incrementAndGet();
            lastToken.set(token);
        }

        @FencepostLock(name = "integration-advisory", lockAtMostFor = "1m", type = LockType.ADVISORY)
        void advisoryLockedMethod() {
            counter.incrementAndGet();
        }

        @FencepostLock(name = "integration-session", lockAtMostFor = "1m", type = LockType.SESSION)
        void sessionLockedMethod() {
            counter.incrementAndGet();
        }
    }
}
