package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RecordValueDefaultsTest {

  @Nested
  class ForSchemalessTest {

    @Test
    void shouldReturnNullWhenSchemaIsNull() {

      // Given & When & Then
      Assertions.assertThat(RecordValueDefaults.forSchemaless()).isNull();
    }
  }

  @Nested
  class ForSchemaTest {

    @Test
    void shouldReturnDefaultValueWhenSchemaHasDefaultValue() {
      // Given
      Schema valueSchema = SchemaBuilder.string().defaultValue("default_string").build();
      SourceRecord record = new SourceRecord(null, null, "test-topic", valueSchema, "original");

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo("default_string");
    }

    @Test
    void shouldReturnNullWhenSchemaIsOptional() {
      // Given
      Schema valueSchema = SchemaBuilder.string().optional().build();
      SourceRecord record = new SourceRecord(null, null, "test-topic", valueSchema, "original");

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isNull();
    }

    @Test
    void shouldReturnDefaultValuesForAllFieldsForStructSchema() {
      // Given
      Schema nestedSchema =
          SchemaBuilder.struct()
              .name("nestedPayload")
              .field("nestedId", Schema.INT64_SCHEMA)
              .field("nestedName", Schema.STRING_SCHEMA)
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
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isNotNull();
      assertThat(placeholderValue).isInstanceOf(Struct.class);
      assertThat(((Struct) placeholderValue).getInt64("id")).isEqualTo(0L);
      assertThat(((Struct) placeholderValue).getString("name")).isEqualTo("");
      assertThat(((Struct) placeholderValue).getStruct("nestedPayload").getInt64("nestedId"))
          .isEqualTo(0L);
      assertThat(((Struct) placeholderValue).getStruct("nestedPayload").getString("nestedName"))
          .isEqualTo("");
    }

    @Test
    void shouldReturnEmptyMapForMapSchema() {
      // Given
      Schema valueSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", valueSchema, Collections.singletonMap("key", 1));

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isNotNull();
      assertThat(placeholderValue).isInstanceOf(Map.class);
      assertThat((Map<?, ?>) placeholderValue).isEmpty();
    }

    @Test
    void shouldReturnEmptyListForArraySchema() {
      // Given
      Schema valueSchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", valueSchema, Collections.singletonList("item"));

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isNotNull();
      assertThat(placeholderValue).isInstanceOf(List.class);
      assertThat((List<?>) placeholderValue).isEmpty();
    }

    @Test
    void shouldReturnDefaultValueForInt8Schema() {
      // Given
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.INT8_SCHEMA, (byte) 1);

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo((byte) 0);
    }

    @Test
    void shouldReturnDefaultValueForInt16Schema() {
      // Given
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.INT16_SCHEMA, (short) 1);

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo((short) 0);
    }

    @Test
    void shouldReturnDefaultValueForInt32Schema() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", Schema.INT32_SCHEMA, 1);

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo(0);
    }

    @Test
    void shouldReturnDefaultValueForInt64Schema() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", Schema.INT64_SCHEMA, 1L);

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo(0L);
    }

    @Test
    void shouldReturnDefaultValueForFloat32Schema() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", Schema.FLOAT32_SCHEMA, 1.0f);

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo(0.0f);
    }

    @Test
    void shouldReturnDefaultValueForFloat64Schema() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", Schema.FLOAT64_SCHEMA, 1.0);

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo(0.0);
    }

    @Test
    void shouldReturnDefaultValueForBooleanSchema() {
      // Given
      SourceRecord record = new SourceRecord(null, null, "test-topic", Schema.BOOLEAN_SCHEMA, true);

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo(false);
    }

    @Test
    void shouldReturnDefaultValueForStringSchema() {
      // Given
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", Schema.STRING_SCHEMA, "hello");

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo("");
    }

    @Test
    void shouldReturnDefaultValueForBytesSchema() {
      // Given
      SourceRecord record =
          new SourceRecord(
              null, null, "test-topic", Schema.BYTES_SCHEMA, ByteBuffer.wrap(new byte[] {1}));

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo(ByteBuffer.wrap(new byte[0]));
    }

    @Test
    void shouldReturnDefaultValueForTimestampSchema() {
      // Given
      Schema timestampSchema = Timestamp.SCHEMA;
      SourceRecord record = new SourceRecord(null, null, "test-topic", timestampSchema, new Date());

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo(new Date(0));
    }

    @Test
    void shouldReturnDefaultValueForDecimalSchema() {
      // Given
      Schema decimalSchema = Decimal.schema(2);
      SourceRecord record =
          new SourceRecord(null, null, "test-topic", decimalSchema, BigDecimal.ONE);

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo(BigDecimal.ZERO);
    }

    @Test
    void shouldReturnDefaultValueForDateSchema() {
      // Given
      Schema dateSchema = org.apache.kafka.connect.data.Date.SCHEMA;
      SourceRecord record = new SourceRecord(null, null, "test-topic", dateSchema, new Date());

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo(new Date(0));
    }

    @Test
    void shouldReturnDefaultValueForTimeSchema() {
      // Given
      Schema timeSchema = Time.SCHEMA;
      SourceRecord record = new SourceRecord(null, null, "test-topic", timeSchema, new Date());

      // When
      Object placeholderValue = RecordValueDefaults.forSchema(record.valueSchema());

      // Then
      assertThat(placeholderValue).isEqualTo(new Date(0));
    }
  }
}
