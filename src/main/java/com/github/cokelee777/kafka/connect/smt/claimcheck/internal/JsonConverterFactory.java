package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import java.util.Map;
import org.apache.kafka.connect.json.JsonConverter;

public class JsonConverterFactory {

  public static JsonConverter createValueConverter() {
    JsonConverter converter = new JsonConverter();
    Map<String, Object> configs = Map.of("schemas.enable", false, "converter.type", "value");
    converter.configure(configs);
    return converter;
  }

  private JsonConverterFactory() {}
}
