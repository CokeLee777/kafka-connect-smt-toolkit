package com.github.cokelee777.kafka.connect.smt.claimcheck.fixture.config;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckSinkTransformConfig;
import java.util.HashMap;
import java.util.Map;

/**
 * Test utility class for building configuration maps used by {@link ClaimCheckSinkTransformConfig}.
 *
 * <p>This fixture simplifies configuration setup for testing Claim Check sink transforms.
 */
public final class ClaimCheckSinkTransformConfigFixture {

  private ClaimCheckSinkTransformConfigFixture() {}

  /**
   * Creates a new {@link Builder} instance.
   *
   * @return a new {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing configuration maps for {@link ClaimCheckSinkTransformConfig}.
   */
  public static class Builder {

    private final Map<String, String> configs = new HashMap<>();

    /**
     * Sets the storage type for the sink transform.
     *
     * @param storageType storage backend type (e.g., S3, filesystem)
     * @return this builder instance
     */
    public Builder storageType(String storageType) {
      configs.put(ClaimCheckSinkTransformConfig.STORAGE_TYPE_CONFIG, storageType);
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
