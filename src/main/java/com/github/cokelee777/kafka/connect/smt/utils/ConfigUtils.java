package com.github.cokelee777.kafka.connect.smt.utils;

import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class ConfigUtils {

    private ConfigUtils() {}

    public static String getRequiredString(SimpleConfig config, String key) {
        String value = config.getString(key);
        if (value == null || value.isBlank()) {
            throw new org.apache.kafka.common.config.ConfigException(
                    "Configuration \"" + key + "\" must not be empty or blank.");
        }
        return value.trim();
    }

    public static String getOptionalString(SimpleConfig config, String key) {
        String value = config.getString(key);
        if (value != null) {
            if (value.isBlank()) {
                throw new org.apache.kafka.common.config.ConfigException(
                        "Configuration \"" + key + "\" must not be empty or blank if provided.");
            }
            return value.trim();
        }
        return null;
    }
}
