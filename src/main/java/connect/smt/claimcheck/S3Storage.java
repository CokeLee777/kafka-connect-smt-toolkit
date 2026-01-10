package connect.smt.claimcheck;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.util.Map;

public class S3Storage implements ClaimCheckStorage {

  public static final String CONFIG_BUCKET_NAME = "claimcheck.s3.bucket.name";
  public static final String CONFIG_REGION = "claimcheck.s3.region";
  public static final String CONFIG_ENDPOINT_OVERRIDE = "claimcheck.s3.endpoint.override";

  private String bucketName;
  private String region;
  private String endpointOverride;

  private S3Client s3Client;

  public S3Storage() {}

  public S3Storage(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getRegion() {
    return region;
  }

  public String getEndpointOverride() {
    return endpointOverride;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.bucketName = getRequiredString(configs, CONFIG_BUCKET_NAME);
    this.region = getOptionalString(configs, CONFIG_REGION);
    this.endpointOverride = getOptionalString(configs, CONFIG_ENDPOINT_OVERRIDE);

    S3ClientBuilder builder =
        S3Client.builder().credentialsProvider(DefaultCredentialsProvider.builder().build());

    if (this.region != null) {
      builder.region(Region.of(this.region));
    } else {
      builder.region(Region.AP_NORTHEAST_2);
    }

    if (this.endpointOverride != null && !this.endpointOverride.isBlank()) {
      builder.endpointOverride(URI.create(this.endpointOverride));
      // http://bucketName.localhost:4566 -> http://localhost:4566/bucketName
      builder.forcePathStyle(true);
    }

    this.s3Client = builder.build();
  }

  private static String getRequiredString(Map<String, ?> configs, String key) {
    Object value = configs.get(key);
    if (value == null) {
      throw new IllegalArgumentException(key + " is required");
    }
    if (!(value instanceof String str)) {
      throw new IllegalArgumentException(key + " must be a String");
    }
    if (str.isBlank()) {
      throw new IllegalArgumentException(key + " must not be blank");
    }
    return str;
  }

  private static String getOptionalString(Map<String, ?> configs, String key) {
    Object value = configs.get(key);
    if (value == null) {
      return null;
    }
    if (!(value instanceof String str)) {
      throw new IllegalArgumentException(key + " must be a String");
    }
    if (str.isBlank()) {
      return null;
    }
    return str;
  }

  @Override
  public String store(String key, byte[] data) {
    if (this.s3Client == null) {
      throw new IllegalStateException("S3Client is not initialized. Call configure() first.");
    }

    try {
      PutObjectRequest putObjectRequest =
          PutObjectRequest.builder().bucket(this.bucketName).key(key).build();
      this.s3Client.putObject(putObjectRequest, RequestBody.fromBytes(data));

      return "s3://" + this.bucketName + "/" + key;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to upload to S3. Bucket: " + this.bucketName + ", Key: " + key, e);
    }
  }

  @Override
  public void close() {
    if (this.s3Client != null) {
      s3Client.close();
    }
  }
}
