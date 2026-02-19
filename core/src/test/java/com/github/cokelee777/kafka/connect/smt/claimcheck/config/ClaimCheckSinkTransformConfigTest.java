package com.github.cokelee777.kafka.connect.smt.claimcheck.config;

import static com.github.cokelee777.kafka.connect.smt.claimcheck.config.ClaimCheckTransformConfig.STORAGE_TYPE_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.github.cokelee777.kafka.connect.smt.claimcheck.fixture.config.ClaimCheckSinkTransformConfigFixture;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ClaimCheckSinkTransformConfigTest {

  @Nested
  class ConstructorTest {

    @Test
    void shouldConstructWithProvidedStorageType() {
      // Given
      Map<String, String> configs =
          ClaimCheckSinkTransformConfigFixture.builder()
              .storageType(ClaimCheckStorageType.FILESYSTEM.type())
              .build();

      // When
      ClaimCheckSinkTransformConfig config = new ClaimCheckSinkTransformConfig(configs);

      // Then
      assertThat(config.getStorageType()).isEqualTo(ClaimCheckStorageType.FILESYSTEM.type());
    }

    @Test
    void shouldThrowConfigExceptionWhenStorageTypeIsMissing() {
      // Given
      Map<String, String> configs = ClaimCheckSinkTransformConfigFixture.builder().build();

      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> new ClaimCheckSinkTransformConfig(configs))
          .withMessageContaining(
              "Missing required configuration \""
                  + STORAGE_TYPE_CONFIG
                  + "\" which has no default value.");
    }
  }

  @Nested
  class ConfigDefValidationTest {

    @Test
    void shouldReturnNonNullConfigDef() {
      // When
      ConfigDef configDef = ClaimCheckSinkTransformConfig.configDef();

      // Then
      assertThat(configDef).isNotNull();
      assertThat(configDef.names()).contains(STORAGE_TYPE_CONFIG);
    }

    @Test
    void shouldValidateStorageTypeConfigAcceptsValidValues() {
      // Given
      Map<String, String> validConfigs =
          ClaimCheckSinkTransformConfigFixture.builder()
              .storageType(ClaimCheckStorageType.FILESYSTEM.type())
              .build();

      // When & Then
      ClaimCheckSinkTransformConfig.configDef().parse(validConfigs);
    }

    @Test
    void shouldValidateStorageTypeConfigRejectsInvalidValues() {
      // Given
      Map<String, String> invalidConfigs = Collections.singletonMap(STORAGE_TYPE_CONFIG, "INVALID");

      // When & Then
      assertThatExceptionOfType(ConfigException.class)
          .isThrownBy(() -> ClaimCheckSinkTransformConfig.configDef().parse(invalidConfigs))
          .withMessageContaining("Invalid value INVALID for configuration " + STORAGE_TYPE_CONFIG);
    }
  }
}
