package com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage;

import com.github.cokelee777.kafka.connect.smt.common.utils.PathUtils;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import software.amazon.awssdk.regions.Region;

/**
 * Configuration for S3 Storage.
 */
public class S3StorageConfig extends AbstractConfig {

  public static final String BUCKET_NAME_CONFIG = "storage.s3.bucket.name";
  public static final String BUCKET_NAME_DOC = "S3 Bucket Name";

  public static final String REGION_CONFIG = "storage.s3.region";
  public static final String REGION_DEFAULT = Region.AP_NORTHEAST_2.id();
  public static final String REGION_DOC = "AWS Region";

  public static final String PATH_PREFIX_CONFIG = "storage.s3.path.prefix";
  public static final String PATH_PREFIX_DEFAULT = "claim-checks";
  public static final String PATH_PREFIX_DOC = "Path prefix for stored objects in S3 bucket";

  public static final String ENDPOINT_OVERRIDE_CONFIG = "storage.s3.endpoint.override";
  public static final String ENDPOINT_OVERRIDE_DOC =
      "S3 Endpoint Override. For testing purposes only (e.g., with LocalStack)";

  public static final String RETRY_MAX_CONFIG = "storage.s3.retry.max";
  public static final int RETRY_MAX_DEFAULT = 3;
  public static final String RETRY_MAX_DOC = "Maximum number of retries for S3 upload failures";

  public static final String RETRY_BACKOFF_MS_CONFIG = "storage.s3.retry.backoff.ms";
  public static final long RETRY_BACKOFF_MS_DEFAULT = 300L;
  public static final String RETRY_BACKOFF_MS_DOC =
      "Initial backoff time in milliseconds between S3 upload retries";

  public static final String RETRY_MAX_BACKOFF_MS_CONFIG = "storage.s3.retry.max.backoff.ms";
  public static final long RETRY_MAX_BACKOFF_MS_DEFAULT = 20_000L;
  public static final String RETRY_MAX_BACKOFF_MS_DOC =
      "Maximum backoff time in milliseconds for S3 upload retries";

  private static final ConfigDef CONFIG;

  static {
    CONFIG = new ConfigDef();
    CONFIG.define(
        BUCKET_NAME_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.NO_DEFAULT_VALUE,
        new ConfigDef.NonEmptyString(),
        ConfigDef.Importance.HIGH,
        BUCKET_NAME_DOC);
    CONFIG.define(
        REGION_CONFIG,
        ConfigDef.Type.STRING,
        REGION_DEFAULT,
        new ConfigDef.NonEmptyString(),
        ConfigDef.Importance.HIGH,
        REGION_DOC);
    CONFIG.define(
        PATH_PREFIX_CONFIG,
        ConfigDef.Type.STRING,
        PATH_PREFIX_DEFAULT,
        new ConfigDef.NonEmptyString(),
        ConfigDef.Importance.MEDIUM,
        PATH_PREFIX_DOC);
    CONFIG.define(
        ENDPOINT_OVERRIDE_CONFIG,
        ConfigDef.Type.STRING,
        null,
        new ConfigDef.NonEmptyString(),
        ConfigDef.Importance.LOW,
        ENDPOINT_OVERRIDE_DOC);
    CONFIG.define(
        RETRY_MAX_CONFIG,
        ConfigDef.Type.INT,
        RETRY_MAX_DEFAULT,
        ConfigDef.Range.atLeast(0),
        ConfigDef.Importance.LOW,
        RETRY_MAX_DOC);
    CONFIG.define(
        RETRY_BACKOFF_MS_CONFIG,
        ConfigDef.Type.LONG,
        RETRY_BACKOFF_MS_DEFAULT,
        ConfigDef.Range.atLeast(1L),
        ConfigDef.Importance.LOW,
        RETRY_BACKOFF_MS_DOC);
    CONFIG.define(
        RETRY_MAX_BACKOFF_MS_CONFIG,
        ConfigDef.Type.LONG,
        RETRY_MAX_BACKOFF_MS_DEFAULT,
        ConfigDef.Range.atLeast(1L),
        ConfigDef.Importance.LOW,
        RETRY_MAX_BACKOFF_MS_DOC);
  }

  private final String bucketName;
  private final String region;
  private final String pathPrefix;
  private final String endpointOverride;
  private final int retryMax;
  private final long retryBackoffMs;
  private final long retryMaxBackoffMs;

  public S3StorageConfig(Map<?, ?> configs) {
    super(CONFIG, configs);
    this.bucketName = getString(BUCKET_NAME_CONFIG);
    this.region = getString(REGION_CONFIG);
    this.pathPrefix = PathUtils.normalizePathPrefix(getString(PATH_PREFIX_CONFIG));
    this.endpointOverride = getString(ENDPOINT_OVERRIDE_CONFIG);
    this.retryMax = getInt(RETRY_MAX_CONFIG);
    this.retryBackoffMs = getLong(RETRY_BACKOFF_MS_CONFIG);
    this.retryMaxBackoffMs = getLong(RETRY_MAX_BACKOFF_MS_CONFIG);
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getRegion() {
    return region;
  }

  public String getPathPrefix() {
    return pathPrefix;
  }

  public String getEndpointOverride() {
    return endpointOverride;
  }

  public int getRetryMax() {
    return retryMax;
  }

  public long getRetryBackoffMs() {
    return retryBackoffMs;
  }

  public long getRetryMaxBackoffMs() {
    return retryMaxBackoffMs;
  }
}
