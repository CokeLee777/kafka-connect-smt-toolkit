package com.github.cokelee777.kafka.connect.smt.claimcheck.defaultvalue;

public enum DefaultValueStrategyType {
  SCHEMALESS("schemaless"),
  DEBEZIUM_STRUCT("debezium_struct"),
  FLAT_STRUCT("flat_struct");

  private final String type;

  DefaultValueStrategyType(String type) {
    this.type = type;
  }

  public String type() {
    return type;
  }
}
