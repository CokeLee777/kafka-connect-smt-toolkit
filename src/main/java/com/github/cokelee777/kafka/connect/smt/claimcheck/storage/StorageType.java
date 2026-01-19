package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

/** Enumerates the supported types of storage backends. */
public enum StorageType {
  /** Amazon S3 storage. */
  S3("s3");

  private final String type;

  StorageType(String type) {
    this.type = type;
  }

  /**
   * Returns the string identifier for the storage type.
   *
   * @return The lower-case type string (e.g., "s3").
   */
  public String type() {
    return type;
  }
}
