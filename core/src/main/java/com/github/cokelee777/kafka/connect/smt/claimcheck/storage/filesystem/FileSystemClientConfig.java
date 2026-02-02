package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem;

/**
 * A configuration class for {@link FileSystemClient}.
 *
 * @param retryMax the maximum number of retries
 * @param retryBackoffMs the initial backoff in milliseconds
 * @param retryMaxBackoffMs the maximum backoff in milliseconds
 */
public record FileSystemClientConfig(int retryMax, long retryBackoffMs, long retryMaxBackoffMs) {}
