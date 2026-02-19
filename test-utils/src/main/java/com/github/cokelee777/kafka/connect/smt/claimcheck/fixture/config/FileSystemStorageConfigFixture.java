package com.github.cokelee777.kafka.connect.smt.claimcheck.fixture.config;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig;
import java.util.HashMap;
import java.util.Map;

/**
 * Test utility class for building configuration maps used by {@link FileSystemStorageConfig}.
 *
 * <p>This fixture simplifies the creation of configuration properties required for {@code
 * FileSystemStorage} in unit and integration tests.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * Map<String, String> configs =
 *     FileSystemStorageConfigFixture.builder()
 *         .path("/tmp/storage")
 *         .retryMax(3)
 *         .retryBackoffMs(1000L)
 *         .retryMaxBackoffMs(5000L)
 *         .build();
 * }</pre>
 */
public final class FileSystemStorageConfigFixture {

  private FileSystemStorageConfigFixture() {}

  /**
   * Creates a new {@link Builder} instance.
   *
   * @return a new {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing configuration maps for {@link FileSystemStorageConfig}.
   *
   * <p>All configuration values are stored as {@link String} representations to match Kafka Connect
   * configuration requirements.
   */
  public static class Builder {

    private final Map<String, String> configs = new HashMap<>();

    /**
     * Sets the storage path.
     *
     * @param path the root directory path for file system storage
     * @return this builder instance
     */
    public Builder path(String path) {
      configs.put(FileSystemStorageConfig.PATH_CONFIG, path);
      return this;
    }

    /**
     * Sets the maximum number of retry attempts.
     *
     * @param retryMax maximum retry count
     * @return this builder instance
     */
    public Builder retryMax(int retryMax) {
      configs.put(FileSystemStorageConfig.RETRY_MAX_CONFIG, String.valueOf(retryMax));
      return this;
    }

    /**
     * Sets the retry backoff time in milliseconds.
     *
     * @param retryBackoffMs retry interval in milliseconds
     * @return this builder instance
     */
    public Builder retryBackoffMs(long retryBackoffMs) {
      configs.put(FileSystemStorageConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs));
      return this;
    }

    /**
     * Sets the maximum retry backoff time in milliseconds.
     *
     * @param retryMaxBackoffMs maximum retry backoff interval in milliseconds
     * @return this builder instance
     */
    public Builder retryMaxBackoffMs(long retryMaxBackoffMs) {
      configs.put(
          FileSystemStorageConfig.RETRY_MAX_BACKOFF_MS_CONFIG, String.valueOf(retryMaxBackoffMs));
      return this;
    }

    /**
     * Builds and returns a new configuration map.
     *
     * <p>The returned map is a defensive copy and can be safely modified without affecting the
     * builder state.
     *
     * @return a new {@link Map} containing the configured properties
     */
    public Map<String, String> build() {
      return new HashMap<>(configs);
    }
  }
}
