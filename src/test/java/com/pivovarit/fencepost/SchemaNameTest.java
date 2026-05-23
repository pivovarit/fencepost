package com.pivovarit.fencepost;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaNameTest {

    @Test
    void bareTableNameShouldReturnFullNameWhenNoDot() {
        assertThat(SchemaManager.bareTableName("fencepost_locks")).isEqualTo("fencepost_locks");
    }

    @Test
    void bareTableNameShouldExtractNameAfterDot() {
        assertThat(SchemaManager.bareTableName("myschema.fencepost_locks")).isEqualTo("fencepost_locks");
    }

    @Test
    void bareTableNameShouldHandleMultipleDots() {
        assertThat(SchemaManager.bareTableName("catalog.myschema.table")).isEqualTo("table");
    }

    @Test
    void schemaNameShouldReturnPublicWhenNoDot() {
        assertThat(SchemaManager.schemaName("fencepost_locks")).isEqualTo("public");
    }

    @Test
    void schemaNameShouldExtractSchemaBeforeDot() {
        assertThat(SchemaManager.schemaName("myschema.fencepost_locks")).isEqualTo("myschema");
    }

    @Test
    void schemaNameShouldPreserveFullPrefixWithMultipleDots() {
        assertThat(SchemaManager.schemaName("catalog.myschema.table")).isEqualTo("catalog.myschema");
    }

    @Test
    void tokenTableNameShouldAppendSuffix() {
        assertThat(TableBasedLock.tokenTableName("fencepost_locks")).isEqualTo("fencepost_locks_tokens");
    }

    @Test
    void tokenTableNameShouldPreserveSchema() {
        assertThat(TableBasedLock.tokenTableName("myschema.fencepost_locks")).isEqualTo("myschema.fencepost_locks_tokens");
    }

    @Test
    void tokenSequenceNameShouldAppendSuffix() {
        assertThat(TableBasedLock.tokenSequenceName("fencepost_locks")).isEqualTo("fencepost_locks_token_seq");
    }

    @Test
    void tokenSequenceNameShouldPreserveSchema() {
        assertThat(TableBasedLock.tokenSequenceName("myschema.fencepost_locks")).isEqualTo("myschema.fencepost_locks_token_seq");
    }

    @Test
    void computeSessionTokenSeqNameShouldQuoteName() {
        assertThat(TableBasedLock.computeSessionTokenSeqName("fencepost_locks", "my_lock"))
          .isEqualTo("\"fencepost_st_my_lock\"");
    }

    @Test
    void computeSessionTokenSeqNameShouldPreserveSchema() {
        assertThat(TableBasedLock.computeSessionTokenSeqName("myschema.fencepost_locks", "my_lock"))
          .isEqualTo("myschema.\"fencepost_st_my_lock\"");
    }
}
