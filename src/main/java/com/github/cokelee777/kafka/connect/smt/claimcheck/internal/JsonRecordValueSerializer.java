package com.github.cokelee777.kafka.connect.smt.claimcheck.internal;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

public class JsonRecordValueSerializer implements RecordValueSerializer {

  private final JsonConverter jsonConverter;

  public JsonRecordValueSerializer(JsonConverter jsonConverter) {
    this.jsonConverter = jsonConverter;
  }

  @Override
  public byte[] serialize(SourceRecord record) {
    Object value = record.value();
    Schema schema = record.valueSchema();

    if (value == null) {
      return null;
    }

    if (value instanceof byte[]) {
      return (byte[]) value;
    }

    if (value instanceof String) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    }

    try {
      return jsonConverter.fromConnectData(record.topic(), schema, value);
    } catch (Exception e) {
      throw new SerializationException("Failed to serialize value", e);
    }
  }
}
