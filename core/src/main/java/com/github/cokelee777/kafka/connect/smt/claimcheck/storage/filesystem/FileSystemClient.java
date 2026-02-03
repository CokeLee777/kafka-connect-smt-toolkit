package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem;

import com.github.cokelee777.kafka.connect.smt.common.retry.RetryConfig;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A client for interacting with the file system. */
public final class FileSystemClient {

  private static final Logger log = LoggerFactory.getLogger(FileSystemClient.class);

  private final int maxAttempts;
  private final long initialBackOffMs;
  private final long maxBackOffMs;

  FileSystemClient(RetryConfig retryConfig) {
    this.maxAttempts = retryConfig.maxAttempts();
    this.initialBackOffMs = retryConfig.initialBackoff().toMillis();
    this.maxBackOffMs = retryConfig.maxBackoff().toMillis();
  }

  /**
   * Writes a payload to a file.
   *
   * @param path the path to the file
   * @param payload the payload to write
   * @throws IOException if an I/O error occurs.
   */
  public void write(Path path, byte[] payload) throws IOException {
    retryWithBackoff(
        () -> {
          Files.write(path, payload);
          return null;
        },
        "write",
        path);
  }

  /**
   * Reads a payload from a file.
   *
   * @param path the path to the file
   * @return the payload
   * @throws IOException if an I/O error occurs.
   */
  public byte[] read(Path path) throws IOException {
    return retryWithBackoff(() -> Files.readAllBytes(path), "read", path);
  }

  //noinspection BusyWait
  private <T> T retryWithBackoff(Callable<T> operation, String opName, Path path)
      throws IOException {
    int attempt = 1;
    long backoff = initialBackOffMs;

    while (true) {
      try {
        return operation.call();
      } catch (IOException e) {
        // Immediately throw non-retriable exceptions
        if (!isRetriable(e) || attempt >= maxAttempts) {
          log.error("Failed to {} file after {} attempts: {}", opName, attempt, path, e);
          throw e;
        }

        log.warn("{} failed on attempt {}, retrying in ~{} ms: {}", opName, attempt, backoff, path);

        long jitteredBackoff = (long) (backoff * (0.75 + Math.random() * 0.5));
        try {
          Thread.sleep(jitteredBackoff);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted during retry backoff", ie);
        }

        attempt++;
        backoff = Math.min(backoff * 2, maxBackOffMs);
      } catch (Exception e) {
        throw new IOException("Unexpected exception", e);
      }
    }
  }

  private boolean isRetriable(IOException e) {
    // Retry only transient errors
    return !(e instanceof FileNotFoundException
        || e instanceof NoSuchFileException
        || e instanceof AccessDeniedException
        || e instanceof FileSystemException
            && e.getMessage() != null
            && e.getMessage().contains("Read-only file system"));
  }
}
