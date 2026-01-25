package com.github.cokelee777.kafka.connect.smt.claimcheck;

import static org.assertj.core.api.Assertions.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
// Add this
// import
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckValue;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3Storage;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

@Testcontainers
@DisplayName("Claim Check SMT E2E 통합 테스트")
class S3ClaimCheckE2EFlowTest {

  private static final String BUCKET_NAME = "test-bucket";

  @Container
  private static final LocalStackContainer localstack =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:0.14.2"))
          .withServices(LocalStackContainer.Service.S3);

  private final ClaimCheckSourceTransform sourceTransform = new ClaimCheckSourceTransform();
  private final ClaimCheckSinkTransform sinkTransform = new ClaimCheckSinkTransform();
  private static S3Client s3Client;

  @BeforeAll
  static void beforeAll() {
    // S3 클라이언트 초기화
    s3Client =
        S3Client.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey())))
            .region(Region.of(localstack.getRegion()))
            .build();

    // 테스트용 S3 버킷 생성
    s3Client.createBucket(builder -> builder.bucket(BUCKET_NAME));
  }

  @AfterAll
  static void afterAll() {
    s3Client.close();
  }

  @BeforeEach
  void beforeEach() {
    schemaValueJsonConverter = new JsonConverter();
    Map<String, Boolean> configs = Map.of("schema.enabled", true);
    schemaValueJsonConverter.configure(configs, false);
  }

  @AfterEach
  void afterEach() {
    sinkTransform.close();
    sourceTransform.close();
  }

  @Test
  @DisplayName("Sink -> Source 전체 흐름에서 메시지가 정상적으로 변환되고 원복되어야 한다.")
  void shouldPerformClaimCheckE2EFlow() throws IOException {
    /** Given: Common */
    // Common config
    Map<String, Object> commonConfig = new HashMap<>();
    commonConfig.put(S3Storage.Config.BUCKET_NAME, BUCKET_NAME);
    commonConfig.put(S3Storage.Config.REGION, localstack.getRegion());
    commonConfig.put(
        S3Storage.Config.ENDPOINT_OVERRIDE,
        localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());

    /** Given: Source */
    // ClaimCheckSourceTransform config
    Map<String, Object> sourceTransformConfig = new HashMap<>(commonConfig);
    sourceTransformConfig.put(ClaimCheckSourceTransform.Config.THRESHOLD_BYTES, 1L);
    sourceTransformConfig.put(
        ClaimCheckSourceTransform.Config.STORAGE_TYPE, ClaimCheckStorageType.S3.type());
    sourceTransform.configure(sourceTransformConfig);

    // SourceRecord 생성
    Schema originalSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build();
    Struct originalValue = new Struct(originalSchema).put("id", 1L).put("name", "cokelee777");
    SourceRecord originalSourceRecord =
        new SourceRecord(null, null, "test-topic", null, null, originalSchema, originalValue);

    /** When: Source */
    SourceRecord claimCheckSourceRecord = sourceTransform.apply(originalSourceRecord);

    /** Then: Source */
    // ClaimCheckSourceRecord 검증
    assertThat(claimCheckSourceRecord).isNotNull();
    assertThat(claimCheckSourceRecord.topic()).isEqualTo("test-topic");
    assertThat(claimCheckSourceRecord.keySchema()).isNull();
    assertThat(claimCheckSourceRecord.key()).isNull();
    assertThat(claimCheckSourceRecord.valueSchema()).isEqualTo(originalSchema);
    assertThat(claimCheckSourceRecord.value()).isNotNull();
    assertThat(claimCheckSourceRecord.value()).isInstanceOf(Struct.class);

    // GenericStructStrategy 적용되어 모든 필드가 기본값으로 설정됨
    assertThat(((Struct) claimCheckSourceRecord.value()).getInt64("id")).isEqualTo(0L);
    assertThat(((Struct) claimCheckSourceRecord.value()).getString("name")).isEqualTo("");

    // ClaimCheckSourceHeader 검증
    Header claimCheckSourceHeader =
        claimCheckSourceRecord.headers().lastWithName(ClaimCheckSchema.NAME);
    assertThat(claimCheckSourceHeader).isNotNull();
    assertThat(claimCheckSourceHeader.key()).isEqualTo(ClaimCheckSchema.NAME);
    assertThat(claimCheckSourceHeader.schema()).isEqualTo(ClaimCheckSchema.SCHEMA);
    assertThat(claimCheckSourceHeader.value()).isInstanceOf(Struct.class);

    // 실제 데이터 검증
    ClaimCheckValue claimCheckValue = ClaimCheckValue.from((Struct) claimCheckSourceHeader.value());
    String referenceUrl = claimCheckValue.getReferenceUrl();
    long originalSizeBytes = claimCheckValue.getOriginalSizeBytes();

    assertThat(referenceUrl).startsWith("s3://" + BUCKET_NAME + "/");
    assertThat(originalSizeBytes).isGreaterThan(0);

    // S3에 실제 데이터가 저장되었는지 확인
    String s3Key = referenceUrl.substring(("s3://" + BUCKET_NAME + "/").length());
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder().bucket(BUCKET_NAME).key(s3Key).build();
    byte[] serializedRecord = s3Client.getObject(getObjectRequest).readAllBytes();
    assertThat(serializedRecord).isNotEmpty();
    assertThat(serializedRecord.length).isEqualTo(originalSizeBytes);

    /** Given: Sink */
    // ClaimCheckSinkTransform config
    Map<String, Object> sinkTransformConfig = new HashMap<>(commonConfig);
    sinkTransformConfig.put(
        ClaimCheckSinkTransform.Config.STORAGE_TYPE, ClaimCheckStorageType.S3.type());
    sinkTransform.configure(sinkTransformConfig);

    // SinkRecord 생성
    Struct sinkValue = new Struct(originalSchema).put("id", 0L).put("name", "");
    SinkRecord sinkRecord =
        new SinkRecord(claimCheckSourceRecord.topic(), 0, null, null, originalSchema, sinkValue, 0);
    claimCheckSourceRecord.headers().forEach(header -> sinkRecord.headers().add(header));

    /** When: Sink */
    SinkRecord restoredSinkRecord = sinkTransform.apply(sinkRecord);

    /** Then: Sink */
    assertThat(restoredSinkRecord).isNotNull();
    assertThat(restoredSinkRecord.topic()).isEqualTo("test-topic");
    assertThat(restoredSinkRecord.keySchema()).isNull();
    assertThat(restoredSinkRecord.key()).isNull();
    assertThat(restoredSinkRecord.valueSchema()).isEqualTo(originalSchema);
    assertThat(restoredSinkRecord.value()).isNotNull();
    assertThat(restoredSinkRecord.value()).isInstanceOf(Struct.class);

    // 복원된 값이 원본과 동일한지 검증
    Struct restoredValue = (Struct) restoredSinkRecord.value();
    assertThat(restoredValue.getInt64("id")).isEqualTo(1L);
    assertThat(restoredValue.getString("name")).isEqualTo("cokelee777");
    assertThat(restoredValue).isEqualTo(originalValue);

    // ClaimCheck 헤더가 여전히 존재하는지 확인
    Header claimCheckSinkHeader = restoredSinkRecord.headers().lastWithName(ClaimCheckSchema.NAME);
    assertThat(claimCheckSinkHeader).isNotNull();
    assertThat(claimCheckSinkHeader.key()).isEqualTo(ClaimCheckSchema.NAME);
    assertThat(claimCheckSinkHeader.schema()).isEqualTo(ClaimCheckSchema.SCHEMA);
    assertThat(claimCheckSinkHeader.value()).isInstanceOf(Struct.class);

    // 헤더의 referenceUrl이 동일한지 검증
    ClaimCheckValue restoredClaimCheckValue =
        ClaimCheckValue.from((Struct) claimCheckSinkHeader.value());
    assertThat(restoredClaimCheckValue.getReferenceUrl()).isEqualTo(referenceUrl);
    assertThat(restoredClaimCheckValue.getOriginalSizeBytes()).isEqualTo(originalSizeBytes);
  }
}
