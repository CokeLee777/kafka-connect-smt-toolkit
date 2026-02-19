package com.github.cokelee777.kafka.connect.smt.claimcheck.fixture.config;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig;
import java.util.HashMap;
import java.util.Map;

/**
 * Test utility class for building configuration maps used by {@link S3StorageConfig}.
 *
 * <p>This fixture simplifies the creation of configuration properties required for S3-based storage
 * in unit and integration tests.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * Map<String, String> configs =
 *     S3StorageConfigFixture.builder()
 *         .bucketName("test-bucket")
 *         .region("ap-northeast-2")
 *         .withDefaultEndpointOverride()
 *         .retryMax(3)
 *         .build();
 * }</pre>
 */
public final class S3StorageConfigFixture {

  private S3StorageConfigFixture() {}

  /** Default endpoint override value used for local testing environments (e.g., LocalStack). */
  public static final String ENDPOINT_OVERRIDE_DEFAULT = "http://localhost:4566";

  /**
   * Creates a new {@link Builder} instance.
   *
   * @return a new {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing configuration maps for {@link S3StorageConfig}.
   *
   * <p>All configuration values are stored as {@link String} representations to match Kafka Connect
   * configuration requirements.
   */
  public static class Builder {

    private final Map<String, String> configs = new HashMap<>();

    /**
     * Sets the S3 bucket name.
     *
     * @param bucketName the target S3 bucket name
     * @return this builder instance
     */
    public Builder bucketName(String bucketName) {
      configs.put(S3StorageConfig.BUCKET_NAME_CONFIG, bucketName);
      return this;
    }

    /**
     * Sets the AWS region.
     *
     * @param region AWS region identifier
     * @return this builder instance
     */
    public Builder region(String region) {
      configs.put(S3StorageConfig.REGION_CONFIG, region);
      return this;
    }

    /**
     * Sets the path prefix within the S3 bucket.
     *
     * @param pathPrefix prefix path for stored objects
     * @return this builder instance
     */
    public Builder pathPrefix(String pathPrefix) {
      configs.put(S3StorageConfig.PATH_PREFIX_CONFIG, pathPrefix);
      return this;
    }

    /**
     * Sets the endpoint override (useful for local testing).
     *
     * @param endpointOverride custom S3 endpoint URL
     * @return this builder instance
     */
    public Builder endpointOverride(String endpointOverride) {
      configs.put(S3StorageConfig.ENDPOINT_OVERRIDE_CONFIG, endpointOverride);
      return this;
    }

    /**
     * Applies the default endpoint override for local testing.
     *
     * @return this builder instance
     */
    public Builder withDefaultEndpointOverride() {
      return endpointOverride(ENDPOINT_OVERRIDE_DEFAULT);
    }

    /**
     * Sets the maximum number of retry attempts.
     *
     * @param retryMax maximum retry count
     * @return this builder instance
     */
    public Builder retryMax(int retryMax) {
      configs.put(S3StorageConfig.RETRY_MAX_CONFIG, String.valueOf(retryMax));
      return this;
    }

    /**
     * Sets the retry backoff time in milliseconds.
     *
     * @param retryBackoffMs retry interval in milliseconds
     * @return this builder instance
     */
    public Builder retryBackoffMs(long retryBackoffMs) {
      configs.put(S3StorageConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs));
      return this;
    }

    /**
     * Sets the maximum retry backoff time in milliseconds.
     *
     * @param retryMaxBackoffMs maximum retry backoff interval in milliseconds
     * @return this builder instance
     */
    public Builder retryMaxBackoffMs(long retryMaxBackoffMs) {
      configs.put(S3StorageConfig.RETRY_MAX_BACKOFF_MS_CONFIG, String.valueOf(retryMaxBackoffMs));
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
