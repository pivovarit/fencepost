package com.pivovarit.fencepost.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "fencepost")
public class FencepostProperties {

    private String tableName = "fencepost_locks";
    private String defaultLockAtMostFor;

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public String getDefaultLockAtMostFor() { return defaultLockAtMostFor; }
    public void setDefaultLockAtMostFor(String defaultLockAtMostFor) { this.defaultLockAtMostFor = defaultLockAtMostFor; }
}
