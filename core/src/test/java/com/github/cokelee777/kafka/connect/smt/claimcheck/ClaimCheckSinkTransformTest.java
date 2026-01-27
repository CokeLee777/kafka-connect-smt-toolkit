package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchemaFields;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3Storage;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("ClaimCheckSinkTransform 단위 테스트")
class ClaimCheckSinkTransformTest {

  @InjectMocks private ClaimCheckSinkTransform transform;
  @Mock private ClaimCheckStorage storage;

  @Nested
  @DisplayName("configure 메서드 테스트")
  class ConfigureTest {

    @Test
    @DisplayName("올바른 설정정보를 구성하면 정상적으로 구성된다.")
    void rightConfig() {
      // Given
      Map<String, String> configs =
          Map.of(
              ClaimCheckSinkTransform.Config.STORAGE_TYPE,
              ClaimCheckStorageType.S3.type(),
              S3Storage.Config.BUCKET_NAME,
              "test-bucket",
              S3Storage.Config.REGION,
              "ap-northeast-2",
              S3Storage.Config.PATH_PREFIX,
              "test/path",
              S3Storage.Config.RETRY_MAX,
              "3",
              S3Storage.Config.RETRY_BACKOFF_MS,
              "300",
              S3Storage.Config.RETRY_MAX_BACKOFF_MS,
              "20000");

      // When
      transform.configure(configs);

      // Then
      assertThat(transform.getStorageType()).isEqualTo(ClaimCheckStorageType.S3.type());
      assertThat(transform.getStorage()).isNotNull();
      assertThat(transform.getRecordSerializer()).isNotNull();
    }
  }

  @Nested
  @DisplayName("apply 메서드 테스트")
  class ApplyTest {

    @BeforeEach
    void beforeEach() {
      Map<String, String> configs =
          Map.of(
              ClaimCheckSinkTransform.Config.STORAGE_TYPE,
              ClaimCheckStorageType.S3.type(),
              S3Storage.Config.BUCKET_NAME,
              "test-bucket",
              S3Storage.Config.REGION,
              "ap-northeast-2",
              S3Storage.Config.PATH_PREFIX,
              "test/path",
              S3Storage.Config.RETRY_MAX,
              "3",
              S3Storage.Config.RETRY_BACKOFF_MS,
              "300",
              S3Storage.Config.RETRY_MAX_BACKOFF_MS,
              "20000");
      transform.configure(configs);
    }

    @Test
    @DisplayName("헤더에 ClaimCheck 참조값이 포함되지 않은 Record를 인자로 넣으면 아무런 동작도 수행하지 않는다.")
    void nonClaimCheckRecordDoesNothing() {
      // Given
      SinkRecord record =
          new SinkRecord("test-topic", 0, Schema.BYTES_SCHEMA, "key", null, null, 0);

      // When
      SinkRecord resultRecord = transform.apply(record);

      // Then
      assertThat(resultRecord).isEqualTo(record);
      verify(storage, never()).retrieve(any());
    }

    @Test
    @DisplayName("헤더에 ClaimCheck 참조값이 포함된 Schemaless SinkRecord를 인자로 넣으면 원본 Record로 대체된다.")
    void schemalessSinkRecordReturnOriginalRecord() {
      // Given
      String fetchedJson = "{\"id\":1,\"name\":\"cokelee777\"}";
      when(storage.retrieve(any())).thenReturn(fetchedJson.getBytes(StandardCharsets.UTF_8));

      String referenceUrl = "s3://test-bucket/test/path/uuid";
      Struct claimCheckValue =
          new Struct(ClaimCheckSchema.SCHEMA)
              .put(ClaimCheckSchemaFields.REFERENCE_URL, referenceUrl)
              .put(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES, fetchedJson.length())
              .put(ClaimCheckSchemaFields.UPLOADED_AT, System.currentTimeMillis());
      SinkRecord record =
          new SinkRecord("test-topic", 0, Schema.BYTES_SCHEMA, "key", null, null, 0);
      record.headers().add(ClaimCheckSchema.NAME, claimCheckValue, ClaimCheckSchema.SCHEMA);

      // When
      SinkRecord originalRecord = transform.apply(record);

      // Then
      Map<String, Object> expectedValue = new HashMap<>();
      expectedValue.put("id", 1L);
      expectedValue.put("name", "cokelee777");

      assertThat(originalRecord).isNotNull();
      assertThat(originalRecord.topic()).isEqualTo("test-topic");
      assertThat(originalRecord.keySchema()).isEqualTo(Schema.BYTES_SCHEMA);
      assertThat(originalRecord.key()).isEqualTo("key");
      assertThat(originalRecord.valueSchema()).isNull();
      assertThat(originalRecord.value()).isNotNull();
      assertThat(originalRecord.value()).isInstanceOf(Map.class);
      assertThat(originalRecord.value()).isEqualTo(expectedValue);
      assertThat(originalRecord.headers().lastWithName(ClaimCheckSchema.NAME)).isNull();
    }

    @Test
    @DisplayName("헤더에 ClaimCheck 참조값이 포함된 Generic Schema SinkRecord를 인자로 넣으면 원본 Record로 대체된다.")
    void genericSchemaSinkRecordReturnOriginalRecord() {
      // Given
      String fetchedJson =
          "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"field\":\"id\"},{\"type\":\"string\",\"field\":\"name\"}],\"optional\":false,\"name\":\"payload\"},\"payload\":{\"id\":1,\"name\":\"cokelee777\"}}";
      when(storage.retrieve(any())).thenReturn(fetchedJson.getBytes(StandardCharsets.UTF_8));

      String referenceUrl = "s3://test-bucket/test/path/uuid";
      Struct claimCheckValue =
          new Struct(ClaimCheckSchema.SCHEMA)
              .put(ClaimCheckSchemaFields.REFERENCE_URL, referenceUrl)
              .put(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES, fetchedJson.length())
              .put(ClaimCheckSchemaFields.UPLOADED_AT, System.currentTimeMillis());

      Schema valueSchema =
          SchemaBuilder.struct()
              .name("payload")
              .field("id", Schema.INT64_SCHEMA)
              .field("name", Schema.STRING_SCHEMA)
              .build();
      SinkRecord record =
          new SinkRecord("test-topic", 0, Schema.BYTES_SCHEMA, "key", valueSchema, null, 0);
      record.headers().add(ClaimCheckSchema.NAME, claimCheckValue, ClaimCheckSchema.SCHEMA);

      // When
      SinkRecord originalRecord = transform.apply(record);

      // Then
      Struct expectedValue = new Struct(valueSchema).put("id", 1L).put("name", "cokelee777");

      assertThat(originalRecord).isNotNull();
      assertThat(originalRecord.topic()).isEqualTo("test-topic");
      assertThat(originalRecord.keySchema()).isEqualTo(Schema.BYTES_SCHEMA);
      assertThat(originalRecord.key()).isEqualTo("key");
      assertThat(originalRecord.valueSchema()).isEqualTo(valueSchema);
      assertThat(originalRecord.value()).isNotNull();
      assertThat(originalRecord.value()).isInstanceOf(Struct.class);
      assertThat(originalRecord.value()).isEqualTo(expectedValue);
      assertThat(originalRecord.headers().lastWithName(ClaimCheckSchema.NAME)).isNull();
    }

    @Test
    @DisplayName("헤더에 ClaimCheck 참조값이 포함된 Debezium Schema SinkRecord를 인자로 넣으면 원본 Record로 대체된다.")
    void debeziumSchemaSinkRecordReturnOriginalRecord() {
      // Given
      String fetchedJson =
          "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"field\":\"id\"},{\"type\":\"string\",\"field\":\"name\"}],\"optional\":true,\"name\":\"test.db.table.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"field\":\"id\"},{\"type\":\"string\",\"field\":\"name\"}],\"optional\":true,\"name\":\"test.db.table.Value\",\"field\":\"after\"},{\"type\":\"string\",\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Envelope\"},\"payload\":{\"before\":{\"id\":1,\"name\":\"before cokelee777\"},\"after\":{\"id\":1,\"name\":\"after cokelee777\"},\"op\":\"c\",\"ts_ms\":1672531200000}}";
      when(storage.retrieve(any())).thenReturn(fetchedJson.getBytes(StandardCharsets.UTF_8));

      String referenceUrl = "s3://test-bucket/test/path/uuid";
      Struct claimCheckValue =
          new Struct(ClaimCheckSchema.SCHEMA)
              .put(ClaimCheckSchemaFields.REFERENCE_URL, referenceUrl)
              .put(ClaimCheckSchemaFields.ORIGINAL_SIZE_BYTES, fetchedJson.length())
              .put(ClaimCheckSchemaFields.UPLOADED_AT, System.currentTimeMillis());

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
      SinkRecord record =
          new SinkRecord("test-topic", 0, Schema.BYTES_SCHEMA, "key", valueSchema, null, 0);
      record.headers().add(ClaimCheckSchema.NAME, claimCheckValue, ClaimCheckSchema.SCHEMA);

      // When
      SinkRecord originalRecord = transform.apply(record);

      // Then
      Struct before = new Struct(rowSchema).put("id", 1L).put("name", "before cokelee777");
      Struct after = new Struct(rowSchema).put("id", 1L).put("name", "after cokelee777");
      Struct expectedValue =
          new Struct(valueSchema)
              .put("before", before)
              .put("after", after)
              .put("op", "c")
              .put("ts_ms", 1672531200000L);

      assertThat(originalRecord).isNotNull();
      assertThat(originalRecord.topic()).isEqualTo("test-topic");
      assertThat(originalRecord.keySchema()).isEqualTo(Schema.BYTES_SCHEMA);
      assertThat(originalRecord.key()).isEqualTo("key");
      assertThat(originalRecord.valueSchema()).isEqualTo(valueSchema);
      assertThat(originalRecord.value()).isNotNull();
      assertThat(originalRecord.value()).isInstanceOf(Struct.class);
      assertThat(originalRecord.value()).isEqualTo(expectedValue);
      assertThat(originalRecord.headers().lastWithName(ClaimCheckSchema.NAME)).isNull();
    }
  }

  @Nested
  @DisplayName("close 메서드 테스트")
  class CloseTest {

    @Test
    @DisplayName("ClaimCheckStorage가 주입된 상태에서 close 호출 시 storage의 close가 호출된다.")
    void shouldCloseInjectedClaimCheckStorage() {
      // Given & When
      transform.close();

      // Then
      verify(storage, times(1)).close();
    }

    @Test
    @DisplayName("ClaimCheckStorage가 null이어도 예외가 발생하지 않는다.")
    void notCauseExceptionAndCloseWhenClaimCheckStorageIsNull() {
      // Given
      transform = new ClaimCheckSinkTransform();

      // When & Then
      assertDoesNotThrow(() -> transform.close());
    }
  }
}
