package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem;

import com.github.cokelee777.kafka.connect.smt.common.retry.RetryConfig;
import java.time.Duration;

/** A factory for creating {@link FileSystemClient} instances. */
public class FileSystemClientFactory {

  private static final int INITIAL_ATTEMPT = 1;

  /**
   * Creates a new {@link FileSystemClient} instance.
   *
   * @param config the configuration for the file system client
   * @return a new {@link FileSystemClient} instance
   */
  public FileSystemClient create(FileSystemClientConfig config) {
    return new FileSystemClient(createRetryConfig(config));
  }

  private RetryConfig createRetryConfig(FileSystemClientConfig config) {
    int maxAttempts = config.retryMax() + INITIAL_ATTEMPT;
    return new RetryConfig(
        maxAttempts,
        Duration.ofMillis(config.retryBackoffMs()),
        Duration.ofMillis(config.retryMaxBackoffMs()));
  }
}
