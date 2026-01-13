package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.unit;

import static org.junit.jupiter.api.Assertions.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.S3Storage;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("S3Storage.configure() 메서드 단위 테스트")
class S3StorageConfigureTest {

  private static final String TEST_BUCKET_NAME = "test-bucket";
  private static final String TEST_REGION_AP_NORTHEAST_1 = "ap-northeast-1";
  private static final String TEST_REGION_AP_NORTHEAST_2 = "ap-northeast-2";
  private static final String TEST_ENDPOINT_LOCALSTACK = "http://localhost:4566";
  private static final String EXPECTED_MISSING_BUCKET_ERROR_MESSAGE =
      "Missing required configuration \"storage.s3.bucket.name\" which has no default value.";

  private S3Storage storage;

  @BeforeEach
  void setUp() {
    storage = new S3Storage();
  }

  @Nested
  @DisplayName("성공 케이스")
  class SuccessCases {

    @Test
    @DisplayName("필수 설정(버킷, 리전)만 제공하면 정상적으로 초기화된다")
    void configureWithRequiredFieldsOnly() {
      // Given
      Map<String, String> configs = createConfigWithBucketAndRegion(TEST_BUCKET_NAME, TEST_REGION_AP_NORTHEAST_1);

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
      Map<String, String> configs = createConfigWithBucketAndEndpoint(TEST_BUCKET_NAME, TEST_ENDPOINT_LOCALSTACK);

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
      Map<String, String> configs = createConfigWithAllFields(
          TEST_BUCKET_NAME, 
          TEST_REGION_AP_NORTHEAST_1, 
          TEST_ENDPOINT_LOCALSTACK
      );

      // When
      storage.configure(configs);

      // Then
      assertAll(
          () -> assertEquals(TEST_BUCKET_NAME, storage.getBucketName()),
          () -> assertEquals(TEST_REGION_AP_NORTHEAST_1, storage.getRegion()),
          () -> assertEquals(TEST_ENDPOINT_LOCALSTACK, storage.getEndpointOverride())
      );
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
      configs.put("storage.s3.region", TEST_REGION_AP_NORTHEAST_2);

      // When & Then
      ConfigException exception = assertThrows(
          ConfigException.class, 
          () -> storage.configure(configs)
      );
      assertEquals(EXPECTED_MISSING_BUCKET_ERROR_MESSAGE, exception.getMessage());
    }

    @Test
    @DisplayName("빈 설정 맵으로 초기화하면 ConfigException이 발생한다")
    void configureWithEmptyMapThrowsException() {
      // Given
      Map<String, String> configs = new HashMap<>();

      // When & Then
      ConfigException exception = assertThrows(
          ConfigException.class, 
          () -> storage.configure(configs)
      );
      assertEquals(EXPECTED_MISSING_BUCKET_ERROR_MESSAGE, exception.getMessage());
    }
  }

  @Nested
  @DisplayName("경계값 테스트")
  class EdgeCases {

    @Test
    @DisplayName("빈 문자열 버킷 이름은 허용되지만 빈 값으로 설정된다")
    void configureWithEmptyBucketName() {
      // Given
      Map<String, String> configs = createConfigWithBucket("");

      // When
      storage.configure(configs);

      // Then
      assertTrue(storage.getBucketName().isEmpty());
    }

    @Test
    @DisplayName("빈 문자열 리전은 허용되지만 빈 값으로 설정된다")
    void configureWithEmptyRegion() {
      // Given
      Map<String, String> configs = createConfigWithBucketAndRegion(TEST_BUCKET_NAME, "");

      // When
      storage.configure(configs);

      // Then
      assertEquals(TEST_BUCKET_NAME, storage.getBucketName());
      assertTrue(storage.getRegion().isEmpty());
    }

    @Test
    @DisplayName("빈 문자열 엔드포인트는 허용되지만 빈 값으로 설정된다")
    void configureWithEmptyEndpoint() {
      // Given
      Map<String, String> configs = createConfigWithBucketAndEndpoint(TEST_BUCKET_NAME, "");

      // When
      storage.configure(configs);

      // Then
      assertEquals(TEST_BUCKET_NAME, storage.getBucketName());
      assertTrue(storage.getEndpointOverride().isEmpty());
    }

    @Test
    @DisplayName("공백이 포함된 버킷 이름도 정상적으로 처리된다")
    void configureWithWhitespaceBucketName() {
      // Given
      String bucketWithWhitespace = "  test-bucket  ";
      Map<String, String> configs = createConfigWithBucket(bucketWithWhitespace);

      // When
      storage.configure(configs);

      // Then
      assertEquals(TEST_BUCKET_NAME, storage.getBucketName());
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

  private Map<String, String> createConfigWithAllFields(String bucket, String region, String endpoint) {
    Map<String, String> configs = new HashMap<>();
    configs.put("storage.s3.bucket.name", bucket);
    configs.put("storage.s3.region", region);
    configs.put("storage.s3.endpoint.override", endpoint);
    return configs;
  }
}
