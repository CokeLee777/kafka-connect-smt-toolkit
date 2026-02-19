package com.github.cokelee777.kafka.connect.smt.claimcheck.fixture.config;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSourceTransformConfig;
import java.util.HashMap;
import java.util.Map;

/**
 * Test utility class for building configuration maps used by {@link
 * ClaimCheckSourceTransformConfig}.
 *
 * <p>This fixture simplifies configuration setup for testing Claim Check source transforms.
 */
public final class ClaimCheckSourceTransformConfigFixture {

  private ClaimCheckSourceTransformConfigFixture() {}

  /**
   * Creates a new {@link Builder} instance.
   *
   * @return a new {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing configuration maps for {@link ClaimCheckSourceTransformConfig}.
   */
  public static class Builder {

    private final Map<String, String> configs = new HashMap<>();

    /**
     * Sets the storage type for the source transform.
     *
     * @param storageType storage backend type (e.g., S3, filesystem)
     * @return this builder instance
     */
    public Builder storageType(String storageType) {
      configs.put(ClaimCheckSourceTransformConfig.STORAGE_TYPE_CONFIG, storageType);
      return this;
    }

    /**
     * Sets the threshold size in bytes.
     *
     * <p>Messages exceeding this threshold may trigger claim check behavior.
     *
     * @param thresholdBytes threshold in bytes
     * @return this builder instance
     */
    public Builder thresholdBytes(int thresholdBytes) {
      configs.put(
          ClaimCheckSourceTransformConfig.THRESHOLD_BYTES_CONFIG, String.valueOf(thresholdBytes));
      return this;
    }

    /**
     * Builds and returns a new configuration map.
     *
     * @return a new {@link Map} containing the configured properties
     */
    public Map<String, String> build() {
      return new HashMap<>(configs);
    }
  }
}
