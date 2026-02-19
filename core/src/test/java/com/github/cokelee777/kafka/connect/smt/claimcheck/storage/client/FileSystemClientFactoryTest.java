package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.fixture.config.FileSystemStorageConfigFixture;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileSystemClientFactoryTest {

  @TempDir Path tempDir;

  @Nested
  class CreateTest {

    @Test
    void shouldCreateFileSystemClient() {
      // Given
      Map<String, String> configs =
          FileSystemStorageConfigFixture.builder().path(tempDir.toString()).build();
      FileSystemStorageConfig config = new FileSystemStorageConfig(configs);

      // When
      FileSystemClient fileSystemClient = FileSystemClientFactory.create(config);

      // Then
      assertThat(fileSystemClient).isNotNull();
    }
  }
}
