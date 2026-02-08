package com.github.cokelee777.kafka.connect.smt.claimcheck.placeholder.type;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Strategy for generating placeholder values when the original payload is offloaded to external
 * storage.
 *
 * <p>Used by the Source Transform to maintain schema compatibility while the actual data is stored
 * externally.
 */
public sealed interface RecordValuePlaceholder
    permits DebeziumStructRecordValuePlaceholder,
        GenericStructRecordValuePlaceholder,
        SchemalessRecordValuePlaceholder {

  /**
   * Returns the placeholder type identifier.
   *
   * @return the placeholder type name
   */
  String getPlaceholderType();

  /**
   * Returns the schema type this placeholder supports.
   *
   * @return the supported schema type, or {@code null} for schemaless records
   */
  Schema.Type getSupportedSchemaType();

  /**
   * Checks if this placeholder can handle the given record.
   *
   * @param record the record to check
   * @return {@code true} if this placeholder can handle the record
   */
  boolean canHandle(SourceRecord record);

  /**
   * Creates a placeholder value that preserves the original schema structure.
   *
   * @param record the source record
   * @return the generated placeholder value
   */
  Object apply(SourceRecord record);
}
