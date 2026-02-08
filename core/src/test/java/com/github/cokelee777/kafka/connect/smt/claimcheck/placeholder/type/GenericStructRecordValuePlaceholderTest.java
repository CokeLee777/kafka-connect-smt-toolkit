package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type;

import static org.assertj.core.api.Assertions.*;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class GenericStructRecordValuePlaceholderTest {

  private GenericStructRecordValuePlaceholder placeholder;

  @BeforeEach
  void setUp() {
    placeholder = new GenericStructRecordValuePlaceholder();
  }

  @Nested
  class ApplyTest {

    @Test
    void shouldReturnDefaultValuesForAllFields() {
      // Given
      Schema nestedSchema =
          SchemaBuilder.struct()
              .name("nestedPayload")
              .field("nestedId", Schema.INT64_SCHEMA)
              .field("nestedName", Schema.STRING_SCHEMA)
              .optional()
              .build();
      Schema valueSchema =
          SchemaBuilder.struct()
              .name("payload")
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .field("nestedPayload", nestedSchema)
              .build();
      Struct nestedValue =
          new Struct(nestedSchema).put("nestedId", 1L).put("nestedName", "nested cokelee777");
      Struct value =
          new Struct(valueSchema)
              .put("id", 1L)
              .put("name", "cokelee777")
              .put("nestedPayload", nestedValue);
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, "key", valueSchema, value);

      // When
      Object defaultValue = placeholder.apply(record);

      // Then
      assertThat(defaultValue).isNotNull();
      assertThat(defaultValue).isInstanceOf(Struct.class);
      assertThat(((Struct) defaultValue).getInt64("id")).isEqualTo(0L);
      assertThat(((Struct) defaultValue).getString("name")).isEqualTo("");
      assertThat(((Struct) defaultValue).getStruct("nestedPayload").getInt64("nestedId"))
          .isEqualTo(0L);
      assertThat(((Struct) defaultValue).getStruct("nestedPayload").getString("nestedName"))
          .isEqualTo("");
    }
  }
}
