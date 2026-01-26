package com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

public interface DefaultValueStrategy {

  String getStrategyType();

  Schema.Type getSupportedSchemaType();

  boolean canHandle(SourceRecord record);

  Object createDefaultValue(SourceRecord record);
}
