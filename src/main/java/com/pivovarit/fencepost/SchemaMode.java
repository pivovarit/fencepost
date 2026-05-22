package com.pivovarit.fencepost;

import javax.sql.DataSource;

public enum SchemaMode {
    NONE,
    CREATE,
    VALIDATE;

    void applyToLocks(DataSource dataSource, String tableName) {
        switch (this) {
            case CREATE -> SchemaManager.createLockSchema(dataSource, tableName);
            case VALIDATE -> SchemaManager.validateLockSchema(dataSource, tableName);
            case NONE -> {}
        }
    }

    void applyToQueue(DataSource dataSource, String tableName) {
        switch (this) {
            case CREATE -> SchemaManager.createQueueSchema(dataSource, tableName);
            case VALIDATE -> SchemaManager.validateQueueSchema(dataSource, tableName);
            case NONE -> {}
        }
    }
}
