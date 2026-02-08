package com.github.cokelee777.kafka.connect.smt.claimcheck.config;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Configuration for the ClaimCheck Source Transformation.
 */
public class ClaimCheckSourceTransformConfig extends ClaimCheckTransformConfig {

  public static final String THRESHOLD_BYTES_CONFIG = "threshold.bytes";
  public static final int THRESHOLD_BYTES_DEFAULT = 1024 * 1024;
  public static final String THRESHOLD_BYTES_DOC = "Payload size threshold in bytes";

  public static final ConfigDef CONFIG;

  static {
    CONFIG = ClaimCheckTransformConfig.newConfigDef();
    CONFIG.define(
        THRESHOLD_BYTES_CONFIG,
        ConfigDef.Type.INT,
        THRESHOLD_BYTES_DEFAULT,
        ConfigDef.Range.atLeast(1),
        ConfigDef.Importance.HIGH,
        THRESHOLD_BYTES_DOC);
  }

  public static ConfigDef configDef() {
    return CONFIG;
  }

  private final int thresholdBytes;

  public ClaimCheckSourceTransformConfig(Map<?, ?> configs) {
    super(CONFIG, configs);
    this.thresholdBytes = getInt(THRESHOLD_BYTES_CONFIG);
  }

  public int getThresholdBytes() {
    return thresholdBytes;
  }
}
