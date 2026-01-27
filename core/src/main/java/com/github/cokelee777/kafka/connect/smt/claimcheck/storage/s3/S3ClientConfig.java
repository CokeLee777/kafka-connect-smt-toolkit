package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import org.apache.kafka.connect.transforms.util.SimpleConfig;

/** Configuration for creating an S3 client. */
public class S3ClientConfig {

  private final String region;
  private final String endpointOverride;
  private final int retryMax;
  private final long retryBackoffMs;
  private final long retryMaxBackoffMs;

  /**
   * Creates an S3 client configuration.
   *
   * @param region AWS region
   * @param endpointOverride optional endpoint override for testing
   * @param retryMax maximum retry attempts
   * @param retryBackoffMs initial backoff in milliseconds
   * @param retryMaxBackoffMs maximum backoff in milliseconds
   */
  private S3ClientConfig(
      String region,
      String endpointOverride,
      int retryMax,
      long retryBackoffMs,
      long retryMaxBackoffMs) {
    this.region = region;
    this.endpointOverride = endpointOverride;
    this.retryMax = retryMax;
    this.retryBackoffMs = retryBackoffMs;
    this.retryMaxBackoffMs = retryMaxBackoffMs;
  }

  /**
   * Creates an S3ClientConfig from a Kafka Connect SimpleConfig.
   *
   * @param config the Kafka Connect configuration
   * @return a new S3ClientConfig instance
   */
  public static S3ClientConfig from(SimpleConfig config) {
    String region = config.getString(S3Storage.Config.REGION);
    String endpointOverride = config.getString(S3Storage.Config.ENDPOINT_OVERRIDE);
    int retryMax = config.getInt(S3Storage.Config.RETRY_MAX);
    long retryBackoffMs = config.getLong(S3Storage.Config.RETRY_BACKOFF_MS);
    long retryMaxBackoffMs = config.getLong(S3Storage.Config.RETRY_MAX_BACKOFF_MS);

    return new S3ClientConfig(
        region, endpointOverride, retryMax, retryBackoffMs, retryMaxBackoffMs);
  }

  /** Returns the AWS region. */
  public String getRegion() {
    return region;
  }

  /** Returns the endpoint override URL, or {@code null} if not set. */
  public String getEndpointOverride() {
    return endpointOverride;
  }

  /** Returns the maximum number of retry attempts. */
  public int getRetryMax() {
    return retryMax;
  }

  /** Returns the initial backoff in milliseconds. */
  public long getRetryBackoffMs() {
    return retryBackoffMs;
  }

  /** Returns the maximum backoff in milliseconds. */
  public long getRetryMaxBackoffMs() {
    return retryMaxBackoffMs;
  }
}
