package com.pivovarit.fencepost.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "fencepost")
public class FencepostProperties {

    private String tableName = "fencepost_locks";
    private String queueTableName = "fencepost_queue";
    private String defaultLockAtMostFor;

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public String getQueueTableName() { return queueTableName; }
    public void setQueueTableName(String queueTableName) { this.queueTableName = queueTableName; }
    public String getDefaultLockAtMostFor() { return defaultLockAtMostFor; }
    public void setDefaultLockAtMostFor(String defaultLockAtMostFor) { this.defaultLockAtMostFor = defaultLockAtMostFor; }
}
