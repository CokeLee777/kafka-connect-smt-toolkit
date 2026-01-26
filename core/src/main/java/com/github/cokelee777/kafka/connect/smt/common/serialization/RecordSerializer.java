package com.github.cokelee777.kafka.connect.smt.common.serialization;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;

public interface RecordSerializer {

  String type();

  byte[] serialize(SourceRecord record);

  SchemaAndValue deserialize(String topic, byte[] payload);
}
