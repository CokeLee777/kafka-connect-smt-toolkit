package com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue.strategies.schemaless;

import static org.assertj.core.api.Assertions.*;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("SchemalessStrategy 단위 테스트")
class SchemalessStrategyTest {

  private SchemalessStrategy schemalessStrategy;

  @BeforeEach
  void beforeEach() {
    schemalessStrategy = new SchemalessStrategy();
  }

  @Nested
  @DisplayName("createDefaultValue 메서드 테스트")
  class CreateDefaultValueTest {

    @Test
    @DisplayName("처리할 수 있는 Record를 인자로 넣으면 null이 반환된다.")
    void rightArgsReturnNull() {
      // Given
      String value = "payload";
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.BYTES_SCHEMA, "key", null, value);

      // When
      Object defaultValue = schemalessStrategy.createDefaultValue(record);

      // Then
      assertThat(defaultValue).isNull();
    }

    @Test
    @DisplayName("처리할 수 없는 Record를 인자로 넣으면 예외가 발생한다.")
    void wrongArgsCauseException() {
      // Given
      String value = "payload";
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", Schema.STRING_SCHEMA, value);

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> schemalessStrategy.createDefaultValue(record))
          .withMessage("Cannot handle record with non-null schema. Expected schemaless record.");
    }
  }
}
