package com.github.cokelee777.kafka.connect.smt.claimcheck.config;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Configuration for the ClaimCheck Sink Transformation.
 */
public class ClaimCheckSinkTransformConfig extends ClaimCheckTransformConfig {

  public static final ConfigDef CONFIG;

  static {
    CONFIG = ClaimCheckTransformConfig.newConfigDef();
  }

  public static ConfigDef configDef() {
    return CONFIG;
  }

  public ClaimCheckSinkTransformConfig(Map<?, ?> configs) {
    super(CONFIG, configs);
  }
}
