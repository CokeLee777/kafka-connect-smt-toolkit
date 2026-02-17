package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.errors;

/** Exception thrown when storing payload data in external storage fails. */
public final class ClaimCheckStoreException extends ClaimCheckStorageException {

  /**
   * Creates a new store exception.
   *
   * @param message error message describing the failure
   * @param cause root cause of the failure
   */
  public ClaimCheckStoreException(String message, Throwable cause) {
    super(message, cause);
  }
}
