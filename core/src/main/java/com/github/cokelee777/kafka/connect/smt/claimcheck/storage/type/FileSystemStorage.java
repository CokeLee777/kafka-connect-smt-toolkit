package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem.FileSystemClient;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem.FileSystemClientConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem.FileSystemClientFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * File system based storage backend for the Claim Check pattern.
 *
 * <p>Stores large payloads to a local or network-mounted file system (e.g., NAS, NFS, SMB) and
 * returns a file:// reference URL for retrieval.
 */
public final class FileSystemStorage implements ClaimCheckStorage {

  public static final class Config {

    public static final String PATH = "storage.filesystem.path";
    public static final String RETRY_MAX = "storage.filesystem.retry.max";
    public static final String RETRY_BACKOFF_MS = "storage.filesystem.retry.backoff.ms";
    public static final String RETRY_MAX_BACKOFF_MS = "storage.filesystem.retry.max.backoff.ms";

    public static final String DEFAULT_PATH = "claim-checks";
    public static final int DEFAULT_RETRY_MAX = 3;
    public static final long DEFAULT_RETRY_BACKOFF_MS = 300L;
    public static final long DEFAULT_MAX_BACKOFF_MS = 20_000L;

    public static final ConfigDef DEFINITION =
        new ConfigDef()
            .define(
                PATH,
                ConfigDef.Type.STRING,
                DEFAULT_PATH,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                "Directory path for storing claim check files. "
                    + "Can be a local path or a network-mounted path (e.g., /claim-checks).")
            .define(
                RETRY_MAX,
                ConfigDef.Type.INT,
                DEFAULT_RETRY_MAX,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.LOW,
                "Maximum number of retries for file upload failures.")
            .define(
                RETRY_BACKOFF_MS,
                ConfigDef.Type.LONG,
                DEFAULT_RETRY_BACKOFF_MS,
                ConfigDef.Range.atLeast(1L),
                ConfigDef.Importance.LOW,
                "Initial backoff time in milliseconds between file upload retries.")
            .define(
                RETRY_MAX_BACKOFF_MS,
                ConfigDef.Type.LONG,
                DEFAULT_MAX_BACKOFF_MS,
                ConfigDef.Range.atLeast(1L),
                ConfigDef.Importance.LOW,
                "Maximum backoff time in milliseconds for file upload retries.");

    public static FileSystemClientConfig toFileSystemClientConfig(SimpleConfig config) {
      return new FileSystemClientConfig(
          config.getInt(RETRY_MAX),
          config.getLong(RETRY_BACKOFF_MS),
          config.getLong(RETRY_MAX_BACKOFF_MS));
    }

    private Config() {}
  }

  private FileSystemClient fileSystemClient;
  private Path storagePath;

  public FileSystemStorage() {}

  public FileSystemStorage(FileSystemClient fileSystemClient) {
    this.fileSystemClient = fileSystemClient;
  }

  public Path getStoragePath() {
    return storagePath;
  }

  @Override
  public String type() {
    return ClaimCheckStorageType.FILESYSTEM.type();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(Config.DEFINITION, configs);

    String path = config.getString(Config.PATH);
    this.storagePath = Paths.get(path).toAbsolutePath().normalize();

    ensureStorageDirectoryExists();

    try {
      this.storagePath = this.storagePath.toRealPath();
    } catch (IOException e) {
      throw new ConfigException(
          "Failed to resolve real path for storage directory: " + this.storagePath, e);
    }

    if (fileSystemClient == null) {
      FileSystemClientConfig fileSystemClientConfig = Config.toFileSystemClientConfig(config);
      FileSystemClientFactory fileSystemClientFactory = new FileSystemClientFactory();
      this.fileSystemClient = fileSystemClientFactory.create(fileSystemClientConfig);
    }
  }

  private void ensureStorageDirectoryExists() {
    try {
      if (!Files.exists(this.storagePath)) {
        Files.createDirectories(this.storagePath);
      }

      if (!Files.isDirectory(this.storagePath)) {
        throw new ConfigException(
            "Storage path exists but is not a directory: " + this.storagePath);
      }

      if (!Files.isWritable(this.storagePath)) {
        throw new ConfigException("Storage directory is not writable: " + this.storagePath);
      }
    } catch (IOException e) {
      throw new ConfigException("Failed to create storage directory: " + this.storagePath, e);
    }
  }

  @Override
  public String store(byte[] payload) {
    checkClientInitialized();

    String filename = generateUniqueFilename();
    Path filePath = this.storagePath.resolve(filename);
    try {
      fileSystemClient.write(filePath, payload);
      return buildReferenceUrl(filePath);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write claim check file: " + filePath, e);
    }
  }

  private String generateUniqueFilename() {
    return UUID.randomUUID().toString();
  }

  private String buildReferenceUrl(Path filePath) {
    return "file://" + filePath.toAbsolutePath().normalize();
  }

  @Override
  public byte[] retrieve(String referenceUrl) {
    checkClientInitialized();

    Path filePath = parsePathFrom(referenceUrl);
    Path realPath = validateAndGetRealPath(filePath);
    try {
      return fileSystemClient.read(realPath);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read claim check file: " + realPath, e);
    }
  }

  private void checkClientInitialized() {
    if (this.fileSystemClient == null) {
      throw new IllegalStateException(
          "FileSystemClient is not configured. Call configure() first.");
    }
  }

  private Path parsePathFrom(String referenceUrl) {
    Objects.requireNonNull(referenceUrl, "referenceUrl must not be null");
    final String prefix = "file://";

    if (!referenceUrl.startsWith(prefix)) {
      throw new IllegalArgumentException("File reference URL must start with 'file://'");
    }

    String pathStr = referenceUrl.substring(prefix.length());
    return Paths.get(pathStr).toAbsolutePath().normalize();
  }

  private Path validateAndGetRealPath(Path filePath) {
    Path realPath;
    try {
      realPath = filePath.toRealPath();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Claim check file does not exist or cannot be accessed: " + filePath, e);
    }

    if (!realPath.startsWith(this.storagePath)) {
      throw new IllegalArgumentException(
          String.format(
              "Resolved file path '%s' is outside the configured storage path '%s'",
              realPath, this.storagePath));
    }

    if (!Files.isRegularFile(realPath)) {
      throw new IllegalArgumentException("Claim check path is not a regular file: " + realPath);
    }

    return realPath;
  }

  @Override
  public void close() {
    // No resources to release for file system storage
  }
}
