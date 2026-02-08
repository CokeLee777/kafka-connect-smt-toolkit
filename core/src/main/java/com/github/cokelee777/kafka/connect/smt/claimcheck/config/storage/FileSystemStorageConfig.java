package com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Configuration for File System Storage.
 */
public class FileSystemStorageConfig extends AbstractConfig {

  public static final String PATH_CONFIG = "storage.filesystem.path";
  public static final String PATH_DEFAULT = "claim-checks";
  public static final String PATH_DOC = "Directory path for storing claim check files";

  public static final String RETRY_MAX_CONFIG = "storage.filesystem.retry.max";
  public static final int RETRY_MAX_DEFAULT = 3;
  public static final String RETRY_MAX_DOC = "Maximum number of retries for file upload failures";

  public static final String RETRY_BACKOFF_MS_CONFIG = "storage.filesystem.retry.backoff.ms";
  public static final long RETRY_BACKOFF_MS_DEFAULT = 300L;
  public static final String RETRY_BACKOFF_MS_DOC =
      "Initial backoff time in milliseconds between file upload retries";

  public static final String RETRY_MAX_BACKOFF_MS_CONFIG =
      "storage.filesystem.retry.max.backoff.ms";
  public static final long RETRY_MAX_BACKOFF_MS_DEFAULT = 20_000L;
  public static final String RETRY_MAX_BACKOFF_MS_DOC =
      "Maximum backoff time in milliseconds for file upload retries";

  private static final ConfigDef CONFIG;

  static {
    CONFIG = new ConfigDef();
    CONFIG.define(
        PATH_CONFIG,
        ConfigDef.Type.STRING,
        PATH_DEFAULT,
        new ConfigDef.NonEmptyString(),
        ConfigDef.Importance.MEDIUM,
        PATH_DOC);
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

  private final String path;
  private final int retryMax;
  private final long retryBackoffMs;
  private final long retryMaxBackoffMs;

  public FileSystemStorageConfig(Map<?, ?> configs) {
    super(CONFIG, configs);
    this.path = getString(PATH_CONFIG);
    this.retryMax = getInt(RETRY_MAX_CONFIG);
    this.retryBackoffMs = getLong(RETRY_BACKOFF_MS_CONFIG);
    this.retryMaxBackoffMs = getLong(RETRY_MAX_BACKOFF_MS_CONFIG);
  }

  public String getPath() {
    return path;
  }

  private Path getAbsolutePath() {
    return Paths.get(path).toAbsolutePath();
  }

  public Path getNormalizedAbsolutePath() {
    return getAbsolutePath().normalize();
  }

  public Path getRealPath() {
    try {
      return getNormalizedAbsolutePath().toRealPath();
    } catch (IOException e) {
      throw new ConfigException("Failed to resolve real path for storage directory: " + path, e);
    }
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
