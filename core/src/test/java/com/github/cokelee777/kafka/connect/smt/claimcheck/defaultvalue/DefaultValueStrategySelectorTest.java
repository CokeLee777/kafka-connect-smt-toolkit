package com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue;

import static org.assertj.core.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("DefaultValueStrategySelector 단위 테스트")
class DefaultValueStrategySelectorTest {

  private DefaultValueStrategySelector defaultValueStrategySelector;

  @BeforeEach
  void beforeEach() {
    defaultValueStrategySelector = new DefaultValueStrategySelector();
  }

  @Nested
  @DisplayName("selectStrategy 메서드 테스트")
  class SelectStrategyTest {

    @Test
    @DisplayName("Debezium Schema SourceRecord를 인자로 넣으면 그에 맞는 DebeziumStructStrategy가 반환된다.")
    void debeziumSchemaReturnDebeziumStructStrategy() {
      // Given
      Schema rowSchema =
          SchemaBuilder.struct()
              .name("test.db.table.Value")
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .optional()
              .build();
      Schema valueSchema =
          SchemaBuilder.struct()
              .name("io.debezium.connector.mysql.Envelope")
              .field("before", rowSchema)
              .field("after", rowSchema)
              .field("op", Schema.STRING_SCHEMA)
              .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
              .build();
      Struct before = new Struct(rowSchema).put("id", 1L).put("name", "before cokelee777");
      Struct after = new Struct(rowSchema).put("id", 1L).put("name", "after cokelee777");
      long tsMs = System.currentTimeMillis();
      Struct envelope =
          new Struct(valueSchema)
              .put("before", before)
              .put("after", after)
              .put("op", "c")
              .put("ts_ms", tsMs);
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", valueSchema, envelope);

      // When
      DefaultValueStrategy strategy = defaultValueStrategySelector.selectStrategy(record);

      // Then
      assertThat(strategy).isNotNull();
      assertThat(strategy.getStrategyType())
          .isEqualTo(DefaultValueStrategyType.DEBEZIUM_STRUCT.type());
    }

    @Test
    @DisplayName("Generic Schema SourceRecord를 인자로 넣으면 그에 맞는 GenericStructStrategy가 반환된다.")
    void genericSchemaReturnGenericStructStrategy() {
      // Given
      Schema valueSchema =
          SchemaBuilder.struct()
              .name("payload")
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .build();
      Struct value = new Struct(valueSchema).put("id", 1L).put("name", "cokelee777");
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", valueSchema, value);

      // When
      DefaultValueStrategy strategy = defaultValueStrategySelector.selectStrategy(record);

      // Then
      assertThat(strategy).isNotNull();
      assertThat(strategy.getStrategyType())
          .isEqualTo(DefaultValueStrategyType.GENERIC_STRUCT.type());
    }

    @Test
    @DisplayName("Schemaless Schema SourceRecord를 인자로 넣으면 그에 맞는 SchemalessStructStrategy가 반환된다.")
    void schemalessSchemaReturnSchemalessStructStrategy() {
      // Given
      Map<String, Object> value = new HashMap<>();
      value.put("id", 1L);
      value.put("name", "cokelee777");
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.BYTES_SCHEMA, "key", null, value);

      // When
      DefaultValueStrategy strategy = defaultValueStrategySelector.selectStrategy(record);

      // Then
      assertThat(strategy).isNotNull();
      assertThat(strategy.getStrategyType()).isEqualTo(DefaultValueStrategyType.SCHEMALESS.type());
    }

    @Test
    @DisplayName("지원하지 않는 Schema SourceRecord를 인자로 넣으면 예외가 발생한다.")
    void unsupportedSchemaSourceRecordCauseException() {
      // Given
      Schema valueSchema = Schema.STRING_SCHEMA;
      String value = "{\"id\":1,\"name\":\"cokelee777\"}";
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", valueSchema, value);

      // When & Then
      assertThatExceptionOfType(IllegalStateException.class)
          .isThrownBy(() -> defaultValueStrategySelector.selectStrategy(record))
          .withMessage("No strategy found for schema: " + valueSchema);
    }

    @Test
    @DisplayName("null SourceRecord를 인자로 넣으면 예외가 발생한다.")
    void nullSourceRecordCauseException() {
      // Given
      SourceRecord record = null;

      // When & Then
      assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> defaultValueStrategySelector.selectStrategy(record))
          .withMessage("Source record cannot be null");
    }
  }
}
