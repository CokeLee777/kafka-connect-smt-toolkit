package com.github.cokelee777.kafka.connect.smt.common.serialization;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;

/** Serializes and deserializes Kafka Connect records for external storage. */
public sealed interface RecordSerializer permits JsonRecordSerializer {

  /**
   * Returns the serializer type identifier.
   *
   * @return the type name (e.g., "json")
   */
  String type();

  /**
   * Serializes a SourceRecord to a byte array.
   *
   * @param record the record to serialize
   * @return the serialized byte array
   */
  byte[] serialize(SourceRecord record);

  /**
   * Deserializes a byte array back to schema and value.
   *
   * @param topic the topic name for deserialization context
   * @param recordBytes the byte array to deserialize
   * @return the deserialized schema and value
   */
  SchemaAndValue deserialize(String topic, byte[] recordBytes);
}
