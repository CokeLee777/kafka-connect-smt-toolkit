package com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue;

public enum DefaultValueStrategyType {
  SCHEMALESS("schemaless"),
  DEBEZIUM_STRUCT("debezium_struct"),
  GENERIC_STRUCT("generic_struct");

  private final String type;

  DefaultValueStrategyType(String type) {
    this.type = type;
  }

  public String type() {
    return type;
  }
}
