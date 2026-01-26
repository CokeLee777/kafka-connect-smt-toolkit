package com.github.cokelee777.kafka.connect.smt.common.retry;

public interface RetryStrategyFactory<T> {

  T create(RetryConfig config);
}
