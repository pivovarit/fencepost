package com.pivovarit.fencepost.spring;

import com.pivovarit.fencepost.Fencepost;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

import javax.sql.DataSource;
import java.time.Duration;

@Configuration
@ConditionalOnClass(Fencepost.class)
@ConditionalOnBean(DataSource.class)
@EnableConfigurationProperties(FencepostProperties.class)
public class FencepostAutoConfiguration implements EmbeddedValueResolverAware {

    private StringValueResolver resolver;

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.resolver = resolver;
    }

    @Bean
    FencepostLockAdvisor fencepostLockAdvisor(DataSource dataSource, FencepostProperties properties) {
        Duration defaultLockAtMostFor = parseDuration(properties.getDefaultLockAtMostFor());

        var interceptor = new FencepostLockInterceptor(
            dataSource,
            properties.getTableName(),
            defaultLockAtMostFor,
            name -> resolver != null ? resolver.resolveStringValue(name) : name
        );

        return new FencepostLockAdvisor(interceptor);
    }

    private static Duration parseDuration(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        return FencepostLockInterceptor.parseDuration(value);
    }
}
