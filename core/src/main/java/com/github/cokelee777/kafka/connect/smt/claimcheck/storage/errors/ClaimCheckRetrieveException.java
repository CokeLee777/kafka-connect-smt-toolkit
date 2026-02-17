package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.errors;

/** Exception thrown when retrieving payload data from external storage fails. */
public final class ClaimCheckRetrieveException extends ClaimCheckStorageException {

  /**
   * Creates a new retrieve exception.
   *
   * @param message error message describing the failure
   * @param cause root cause of the failure
   */
  public ClaimCheckRetrieveException(String message, Throwable cause) {
    super(message, cause);
  }
}
