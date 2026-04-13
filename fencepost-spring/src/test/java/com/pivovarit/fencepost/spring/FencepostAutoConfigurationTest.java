package com.pivovarit.fencepost.spring;

import com.pivovarit.fencepost.Fencepost;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class FencepostAutoConfigurationTest {

    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:17");

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(FencepostAutoConfiguration.class));

    @Test
    void shouldCreateBeansWhenDataSourcePresent() {
        runner
            .withBean(DataSource.class, FencepostAutoConfigurationTest::dataSource)
            .run(context -> {
                assertThat(context).hasSingleBean(FencepostLockAdvisor.class);
            });
    }

    @Test
    void shouldNotCreateBeansWhenDataSourceMissing() {
        runner
            .run(context -> {
                assertThat(context).doesNotHaveBean(FencepostLockAdvisor.class);
            });
    }

    @Test
    void shouldApplyCustomTableName() {
        runner
            .withBean(DataSource.class, FencepostAutoConfigurationTest::dataSource)
            .withPropertyValues("fencepost.table-name=custom_locks")
            .run(context -> {
                assertThat(context).hasSingleBean(FencepostLockAdvisor.class);
            });
    }

    static DataSource dataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        return ds;
    }
}
