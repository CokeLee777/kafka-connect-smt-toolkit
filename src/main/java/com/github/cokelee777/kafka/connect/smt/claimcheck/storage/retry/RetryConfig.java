package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry;

import java.time.Duration;

/** Holds the configuration for a retry mechanism. */
public class RetryConfig {

  private final int maxAttempts;
  private final Duration initialBackoff;
  private final Duration maxBackoff;

  /**
   * Constructs a new RetryConfig.
   *
   * @param maxAttempts The maximum number of attempts to be made.
   * @param initialBackoff The initial duration to wait before the first retry.
   * @param maxBackoff The maximum duration to wait between retries.
   */
  public RetryConfig(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {
    this.maxAttempts = maxAttempts;
    this.initialBackoff = initialBackoff;
    this.maxBackoff = maxBackoff;
  }

  public int maxAttempts() {
    return maxAttempts;
  }

  public Duration initialBackoff() {
    return initialBackoff;
  }

  public Duration maxBackoff() {
    return maxBackoff;
  }
}
