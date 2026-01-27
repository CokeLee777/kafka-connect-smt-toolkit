package com.github.cokelee777.kafka.connect.smt.common.retry;

/**
 * Factory for creating retry strategies.
 *
 * @param <T> the type of retry strategy to create
 */
public interface RetryStrategyFactory<T> {

  /**
   * Creates a retry strategy based on the given configuration.
   *
   * @param config the retry configuration
   * @return a configured retry strategy instance
   */
  T create(RetryConfig config);
}
