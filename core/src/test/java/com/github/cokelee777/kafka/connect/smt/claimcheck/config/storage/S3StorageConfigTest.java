package com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage;

import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.github.cokelee777.kafka.connect.smt.claimcheck.fixture.config.S3StorageConfigFixture;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

class S3StorageConfigTest {

  @Nested
  class ConstructorTest {

    @Test
    void shouldConstructWithAllProvidedArguments() {
      // Given
      Map<String, String> configs =
          S3StorageConfigFixture.builder()
              .bucketName("test-bucket")
              .region(Region.AP_NORTHEAST_1.id())
              .pathPrefix("/test/path")
              .retryMax(5)
              .retryBackoffMs(500L)
              .retryMaxBackoffMs(30000L)
              .withDefaultEndpointOverride()
              .build();

      // When
      S3StorageConfig config = new S3StorageConfig(configs);

      // Then
      assertThat(config.getBucketName()).isEqualTo("test-bucket");
      assertThat(config.getRegion()).isEqualTo(Region.AP_NORTHEAST_1.id());
      assertThat(config.getPathPrefix()).isEqualTo("test/path");
      assertThat(config.getEndpointOverride())
          .isEqualTo(S3StorageConfigFixture.ENDPOINT_OVERRIDE_DEFAULT);
      assertThat(config.getRetryMax()).isEqualTo(5);
      assertThat(config.getRetryBackoffMs()).isEqualTo(500L);
      assertThat(config.getRetryMaxBackoffMs()).isEqualTo(30000L);
    }

    @Test
    void shouldUseDefaultValuesWhenOptionalArgumentsNotProvided() {
      // Given
      Map<String, String> configs =
          S3StorageConfigFixture.builder().bucketName("test-bucket").build();

      // When
      S3StorageConfig config = new S3StorageConfig(configs);

      // Then
      assertThat(config.getBucketName()).isEqualTo("test-bucket");
      assertThat(config.getRegion()).isEqualTo(REGION_DEFAULT);
      assertThat(config.getPathPrefix()).isEqualTo(PATH_PREFIX_DEFAULT);
      assertThat(config.getEndpointOverride()).isNull();
      assertThat(config.getRetryMax()).isEqualTo(RETRY_MAX_DEFAULT);
      assertThat(config.getRetryBackoffMs()).isEqualTo(RETRY_BACKOFF_MS_DEFAULT);
      assertThat(config.getRetryMaxBackoffMs()).isEqualTo(RETRY_MAX_BACKOFF_MS_DEFAULT);
    }

    @Test
    void shouldThrowConfigExceptionWhenBucketNameIsMissing() {
      // Given
      Map<String, String> configs = S3StorageConfigFixture.builder().build();

      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> new S3StorageConfig(configs))
          .withMessageContaining(
              "Missing required configuration \""
                  + BUCKET_NAME_CONFIG
                  + "\" which has no default value.");
    }

    @Test
    void shouldThrowConfigExceptionWhenBucketNameIsEmpty() {
      // Given
      Map<String, String> configs = S3StorageConfigFixture.builder().bucketName("").build();

      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> new S3StorageConfig(configs))
          .withMessage(
              "Invalid value  for configuration "
                  + BUCKET_NAME_CONFIG
                  + ": String must be non-empty");
    }
  }
}
