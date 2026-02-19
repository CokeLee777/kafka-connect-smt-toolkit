package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.client;

import static org.assertj.core.api.Assertions.*;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.fixture.config.S3StorageConfigFixture;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

class S3ClientFactoryTest {

  @Nested
  class CreateTest {

    @Test
    void shouldCreateS3Client() {
      // Given
      Map<String, String> configs =
          S3StorageConfigFixture.builder().bucketName("test-bucket").build();
      S3StorageConfig config = new S3StorageConfig(configs);

      // When
      try (S3Client s3Client = S3ClientFactory.create(config)) {

        // Then
        assertThat(s3Client).isNotNull();
      }
    }

    @Test
    void shouldCreateS3ClientWithEndpointOverride() {
      // Given
      Map<String, String> configs =
          S3StorageConfigFixture.builder()
              .bucketName("test-bucket")
              .withDefaultEndpointOverride()
              .build();
      S3StorageConfig config = new S3StorageConfig(configs);

      // When
      try (S3Client s3Client = S3ClientFactory.create(config)) {

        // Then
        assertThat(s3Client).isNotNull();
      }
    }
  }
}
