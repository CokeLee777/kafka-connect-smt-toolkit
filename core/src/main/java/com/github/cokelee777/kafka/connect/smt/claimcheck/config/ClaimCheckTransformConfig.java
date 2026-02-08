package com.github.cokelee777.kafka.connect.smt.claimcheck.config;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Common configuration for ClaimCheck transformations.
 */
public abstract class ClaimCheckTransformConfig extends AbstractConfig {

  public static final String STORAGE_TYPE_CONFIG = "storage.type";
  public static final String STORAGE_TYPE_DOC =
      "The type of storage implementation to use for the Claim Check pattern";

  /**
   * Create a new ConfigDef with common configurations.
   *
   * @return A ConfigDef object with common configurations.
   */
  public static ConfigDef newConfigDef() {
    ConfigDef config = new ConfigDef();
    config.define(
        STORAGE_TYPE_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.NO_DEFAULT_VALUE,
        ConfigDef.ValidString.in(
            ClaimCheckStorageType.S3.type(), ClaimCheckStorageType.FILESYSTEM.type()),
        ConfigDef.Importance.HIGH,
        STORAGE_TYPE_DOC);

    return config;
  }

  private final String storageType;

  protected ClaimCheckTransformConfig(ConfigDef configDef, Map<?, ?> configs) {
    super(configDef, configs, true);
    this.storageType = getString(STORAGE_TYPE_CONFIG);
  }

  public String getStorageType() {
    return storageType;
  }
}
