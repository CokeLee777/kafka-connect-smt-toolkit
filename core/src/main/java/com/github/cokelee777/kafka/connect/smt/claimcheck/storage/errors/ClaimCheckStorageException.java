package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.errors;

import org.apache.kafka.connect.errors.ConnectException;

/** Base exception type for claim check storage operation failures. */
public abstract class ClaimCheckStorageException extends ConnectException {

  /**
   * Creates a new storage exception.
   *
   * @param message error message describing the failure
   * @param cause root cause of the failure
   */
  protected ClaimCheckStorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
