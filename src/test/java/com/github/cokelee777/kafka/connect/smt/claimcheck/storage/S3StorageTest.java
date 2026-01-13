package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;

@ExtendWith(MockitoExtension.class)
@DisplayName("S3Storage 단위 테스트")
class S3StorageTest {

  private static final String TEST_BUCKET_NAME = "my-bucket";
  private static final String TEST_REGION_AP_NORTHEAST_1 = "ap-northeast-1";
  private static final String TEST_REGION_AP_NORTHEAST_2 = "ap-northeast-2";
  private static final String TEST_ENDPOINT_LOCALSTACK = "http://localhost:4566";
  private static final String EXPECTED_MISSING_BUCKET_ERROR_MESSAGE =
      "Missing required configuration \"storage.s3.bucket.name\" which has no default value.";
  private static final String EXPECTED_EMPTY_BUCKET_ERROR_MESSAGE =
      "Configuration \"storage.s3.bucket.name\" must not be empty or blank.";
  private static final String EXPECTED_EMPTY_REGION_ERROR_MESSAGE =
          "Configuration \"storage.s3.region\" must not be empty or blank if provided.";
  private static final String EXPECTED_EMPTY_ENDPOINT_OVERRIDE_ERROR_MESSAGE =
          "Configuration \"storage.s3.endpoint.override\" must not be empty or blank if provided.";
  

  @Mock
  private S3Client s3Client;

  private S3Storage storage;

  @BeforeEach
  void setUp() {
    storage = new S3Storage(s3Client);
  }

  @Nested
  @DisplayName("성공 케이스")
  class SuccessCases {

    @Test
    @DisplayName("필수 설정(버킷, 리전)만 제공하면 정상적으로 초기화된다")
    void configureWithRequiredFieldsOnly() {
      // Given
      Map<String, String> configs =
          createConfigWithBucketAndRegion(TEST_BUCKET_NAME, TEST_REGION_AP_NORTHEAST_1);

      // When
      storage.configure(configs);

      // Then
      assertEquals(TEST_BUCKET_NAME, storage.getBucketName());
      assertEquals(TEST_REGION_AP_NORTHEAST_1, storage.getRegion());
      assertNull(storage.getEndpointOverride());
    }

    @Test
    @DisplayName("버킷만 제공하면 기본 리전(ap-northeast-2)으로 초기화된다")
    void configureWithBucketOnlyUsesDefaultRegion() {
      // Given
      Map<String, String> configs = createConfigWithBucket(TEST_BUCKET_NAME);

      // When
      storage.configure(configs);

      // Then
      assertEquals(TEST_BUCKET_NAME, storage.getBucketName());
      assertEquals(TEST_REGION_AP_NORTHEAST_2, storage.getRegion());
      assertNull(storage.getEndpointOverride());
    }

    @Test
    @DisplayName("엔드포인트 오버라이드 설정이 정상적으로 파싱된다")
    void configureWithEndpointOverride() {
      // Given
      Map<String, String> configs =
          createConfigWithBucketAndEndpoint(TEST_BUCKET_NAME, TEST_ENDPOINT_LOCALSTACK);

      // When
      storage.configure(configs);

      // Then
      assertEquals(TEST_BUCKET_NAME, storage.getBucketName());
      assertEquals(TEST_ENDPOINT_LOCALSTACK, storage.getEndpointOverride());
    }

    @Test
    @DisplayName("모든 설정값이 제공되면 정상적으로 초기화된다")
    void configureWithAllFields() {
      // Given
      Map<String, String> configs =
          createConfigWithAllFields(
              TEST_BUCKET_NAME, TEST_REGION_AP_NORTHEAST_1, TEST_ENDPOINT_LOCALSTACK);

      // When
      storage.configure(configs);

      // Then
      assertAll(
          () -> assertEquals(TEST_BUCKET_NAME, storage.getBucketName()),
          () -> assertEquals(TEST_REGION_AP_NORTHEAST_1, storage.getRegion()),
          () -> assertEquals(TEST_ENDPOINT_LOCALSTACK, storage.getEndpointOverride()));
    }
  }

  @Nested
  @DisplayName("실패 케이스")
  class FailureCases {

    @Test
    @DisplayName("버킷 이름이 없으면 ConfigException이 발생한다")
    void configureWithoutBucketThrowsException() {
      // Given
      Map<String, String> configs = new HashMap<>();
      configs.put("storage.s3.region", TEST_REGION_AP_NORTHEAST_1);

      // When & Then
      ConfigException exception =
          assertThrows(ConfigException.class, () -> storage.configure(configs));
      assertEquals(EXPECTED_MISSING_BUCKET_ERROR_MESSAGE, exception.getMessage());
    }

    @Test
    @DisplayName("빈 설정 맵으로 초기화하면 ConfigException이 발생한다")
    void configureWithEmptyMapThrowsException() {
      // Given
      Map<String, String> configs = new HashMap<>();

      // When & Then
      ConfigException exception =
          assertThrows(ConfigException.class, () -> storage.configure(configs));
      assertEquals(EXPECTED_MISSING_BUCKET_ERROR_MESSAGE, exception.getMessage());
    }

    @Test
    @DisplayName("빈 문자열 버킷 이름이면 ConfigException이 발생한다")
    void configureWithEmptyBucketName() {
      // Given
      Map<String, String> configs = createConfigWithBucket("");

      // When & Then
      ConfigException exception =
          assertThrows(ConfigException.class, () -> storage.configure(configs));
      assertEquals(EXPECTED_EMPTY_BUCKET_ERROR_MESSAGE, exception.getMessage());
    }

    @Test
    @DisplayName("공백으로만 된 버킷 이름이면 ConfigException이 발생한다")
    void configureWithBlankBucketName() {
      // Given
      Map<String, String> configs = createConfigWithBucket("   ");

      // When & Then
      ConfigException exception =
          assertThrows(ConfigException.class, () -> storage.configure(configs));
      assertEquals(EXPECTED_EMPTY_BUCKET_ERROR_MESSAGE, exception.getMessage());
    }

    @Test
    @DisplayName("빈 문자열 엔드포인트면 ConfigException이 발생한다")
    void configureWithEmptyEndpoint() {
      // Given
      Map<String, String> configs = createConfigWithBucketAndEndpoint(TEST_BUCKET_NAME, "");

      // When & Then
      ConfigException exception =
          assertThrows(ConfigException.class, () -> storage.configure(configs));
      assertEquals(EXPECTED_EMPTY_ENDPOINT_OVERRIDE_ERROR_MESSAGE, exception.getMessage());
    }

    @Test
    @DisplayName("공백으로만 된 엔드포인트면 ConfigException이 발생한다")
    void configureWithBlankEndpoint() {
      // Given
      Map<String, String> configs = createConfigWithBucketAndEndpoint(TEST_BUCKET_NAME, "   ");

      // When & Then
      ConfigException exception =
          assertThrows(ConfigException.class, () -> storage.configure(configs));
      assertEquals(EXPECTED_EMPTY_ENDPOINT_OVERRIDE_ERROR_MESSAGE, exception.getMessage());
    }
  }

  @Nested
  @DisplayName("경계값 테스트")
  class EdgeCases {

    @Test
    @DisplayName("공백이 포함된 버킷 이름도 정상적으로 처리된다")
    void configureWithWhitespaceBucketName() {
      // Given
      String bucketWithWhitespace = "  my-bucket  ";
      Map<String, String> configs = createConfigWithBucket(bucketWithWhitespace);

      // When
      storage.configure(configs);

      // Then
      assertEquals(TEST_BUCKET_NAME, storage.getBucketName());
    }

    @Test
    @DisplayName("빈 문자열 리전이면 ConfigException이 발생한다")
    void configureWithEmptyRegion() {
      // Given
      Map<String, String> configs = createConfigWithBucketAndRegion(TEST_BUCKET_NAME, "");

      // When & Then
      ConfigException exception =
              assertThrows(ConfigException.class, () -> storage.configure(configs));
      assertEquals(EXPECTED_EMPTY_REGION_ERROR_MESSAGE, exception.getMessage());
    }

    @Test
    @DisplayName("공백으로만 된 리전이면 ConfigException이 발생한다")
    void configureWithBlankRegion() {
      // Given
      Map<String, String> configs = createConfigWithBucketAndRegion(TEST_BUCKET_NAME, "   ");

      // When & Then
      ConfigException exception =
              assertThrows(ConfigException.class, () -> storage.configure(configs));
      assertEquals(EXPECTED_EMPTY_REGION_ERROR_MESSAGE, exception.getMessage());
    }
  }

  // 테스트 데이터 생성 헬퍼 메서드
  private Map<String, String> createConfigWithBucket(String bucket) {
    Map<String, String> configs = new HashMap<>();
    configs.put("storage.s3.bucket.name", bucket);
    return configs;
  }

  private Map<String, String> createConfigWithBucketAndRegion(String bucket, String region) {
    Map<String, String> configs = new HashMap<>();
    configs.put("storage.s3.bucket.name", bucket);
    configs.put("storage.s3.region", region);
    return configs;
  }

  private Map<String, String> createConfigWithBucketAndEndpoint(String bucket, String endpoint) {
    Map<String, String> configs = new HashMap<>();
    configs.put("storage.s3.bucket.name", bucket);
    configs.put("storage.s3.endpoint.override", endpoint);
    return configs;
  }

  private Map<String, String> createConfigWithAllFields(
      String bucket, String region, String endpoint) {
    Map<String, String> configs = new HashMap<>();
    configs.put("storage.s3.bucket.name", bucket);
    configs.put("storage.s3.region", region);
    configs.put("storage.s3.endpoint.override", endpoint);
    return configs;
  }
}
