package com.pivovarit.fencepost;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableNameValidationTest {

    @ParameterizedTest
    @ValueSource(strings = {
      "fencepost_locks",
      "my_table",
      "Table1",
      "_private",
      "myschema.my_table",
      "public.fencepost_locks",
      "a",
      "A"
    })
    void shouldAcceptValidTableNames(String tableName) {
        assertThatNoException().isThrownBy(() -> Fencepost.validateTableName(tableName));
    }

    @ParameterizedTest
    @ValueSource(strings = {
      "123table",
      "1abc",
      "0_start"
    })
    void shouldRejectTableNamesStartingWithDigit(String tableName) {
        assertThatThrownBy(() -> Fencepost.validateTableName(tableName))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Invalid table name");
    }

    @ParameterizedTest
    @ValueSource(strings = {
      "my-table",
      "table name",
      "table;drop",
      "my.1table",
      "schema.",
      ".table",
      ""
    })
    void shouldRejectInvalidTableNames(String tableName) {
        assertThatThrownBy(() -> Fencepost.validateTableName(tableName))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Invalid table name");
    }

    @Test
    void shouldRejectNullTableName() {
        assertThatThrownBy(() -> Fencepost.validateTableName(null))
          .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldAcceptSchemaQualifiedNames() {
        assertThatNoException().isThrownBy(() -> Fencepost.validateTableName("myschema.mytable"));
    }

    @Test
    void shouldAcceptMultiLevelSchemaQualifiedNames() {
        assertThatNoException().isThrownBy(() -> Fencepost.validateTableName("catalog.schema.table"));
    }
}
