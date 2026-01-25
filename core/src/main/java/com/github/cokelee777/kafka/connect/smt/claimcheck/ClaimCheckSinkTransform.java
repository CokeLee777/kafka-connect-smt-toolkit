package com.github.cokelee777.kafka.connect.smt.claimcheck;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckValue;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.common.serialization.RecordSerializer;
import com.github.cokelee777.kafka.connect.smt.common.serialization.RecordSerializerFactory;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClaimCheckSinkTransform implements Transformation<SinkRecord> {

  private static final Logger log = LoggerFactory.getLogger(ClaimCheckSinkTransform.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static class Config {

    public static final String STORAGE_TYPE = "storage.type";

    public static final ConfigDef DEFINITION =
        new ConfigDef()
            .define(
                STORAGE_TYPE,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.ValidString.in(ClaimCheckStorageType.S3.type()),
                ConfigDef.Importance.HIGH,
                "Storage implementation type");

    private Config() {}
  }

  private static class TransformConfig extends AbstractConfig {
    TransformConfig(Map<String, ?> originals) {
      super(ClaimCheckSinkTransform.Config.DEFINITION, originals);
    }
  }

  private String storageType;
  private ClaimCheckStorage storage;
  private RecordSerializer recordSerializer;

  public ClaimCheckSinkTransform() {}

  public ClaimCheckSinkTransform(ClaimCheckStorage storage) {
    this.storage = storage;
  }

  public ClaimCheckSinkTransform(RecordSerializer recordSerializer) {
    this.recordSerializer = recordSerializer;
  }

  public ClaimCheckSinkTransform(ClaimCheckStorage storage, RecordSerializer recordSerializer) {
    this.storage = storage;
    this.recordSerializer = recordSerializer;
  }

  public String getStorageType() {
    return storageType;
  }

  public ClaimCheckStorage getStorage() {
    return storage;
  }

  public RecordSerializer getRecordSerializer() {
    return recordSerializer;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    TransformConfig config = new TransformConfig(configs);

    this.storageType = config.getString(Config.STORAGE_TYPE);

    if (this.storage == null) {
      this.storage = ClaimCheckStorageFactory.create(this.storageType);
    }
    Objects.requireNonNull(this.storage, "ClaimCheckStorage not configured");
    this.storage.configure(configs);

    if (this.recordSerializer == null) {
      this.recordSerializer = RecordSerializerFactory.create();
    }
    Objects.requireNonNull(this.recordSerializer, "RecordSerializer not configured");
  }

  @Override
  public SinkRecord apply(SinkRecord record) {
    if (record == null) {
      return null;
    }

    Header claimCheckHeader = record.headers().lastWithName(ClaimCheckSchema.NAME);
    if (claimCheckHeader == null) {
      log.debug("No claim-check header found for record from topic: {}", record.topic());
      return record;
    }

    return createOriginalRecord(record, claimCheckHeader);
  }

  private SinkRecord createOriginalRecord(SinkRecord record, Header claimCheckHeader) {
    ClaimCheckValue claimCheckValue = extractReference(claimCheckHeader);
    String referenceUrl = claimCheckValue.getReferenceUrl();
    Long originalSizeBytes = claimCheckValue.getOriginalSizeBytes();

    log.debug(
        "Recovering claim check record from: {}, original size: {} bytes",
        referenceUrl,
        originalSizeBytes);

    byte[] serializedRecord = this.storage.retrieve(referenceUrl);
    if (serializedRecord == null) {
      throw new ConnectException("Failed to retrieve data from: " + referenceUrl);
    }

    if (serializedRecord.length != originalSizeBytes) {
      log.warn(
          "Size mismatch! Expected: {} bytes, Retrieved: {} bytes",
          originalSizeBytes,
          serializedRecord.length);
    }

    SchemaAndValue schemaAndValue =
        this.recordSerializer.deserialize(record.topic(), serializedRecord);
    if (schemaAndValue == null) {
      log.warn(
          "Failed to restore original record from claim check reference (topic={}). Returning default value record.",
          record.topic());
      return record;
    }

    SinkRecord originalRecord =
        record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            schemaAndValue.schema(),
            schemaAndValue.value(),
            record.timestamp());

    log.debug("Successfully recovered claim check record. Size: {} bytes", serializedRecord.length);

    originalRecord.headers().remove(ClaimCheckSchema.NAME);
    return originalRecord;
  }

  private ClaimCheckValue extractReference(Header header) {
    Object value = header.value();
    if (value instanceof Struct) {
      return parseFromStruct((Struct) value);
    }

    if (value instanceof String) {
      return parseFromJson((String) value);
    }

    if (value instanceof Map) {
      return parseFromMap((Map<?, ?>) value);
    }

    throw new ConnectException("Unsupported claim check header type: " + value.getClass());
  }

  private ClaimCheckValue parseFromStruct(Struct struct) {
    try {
      return ClaimCheckValue.from(struct);
    } catch (Exception e) {
      throw new ConnectException("Failed to parse claim check header STRUCT", e);
    }
  }

  private ClaimCheckValue parseFromJson(String json) {
    try {
      JsonNode node = OBJECT_MAPPER.readTree(json);
      return ClaimCheckValue.from(node);
    } catch (Exception e) {
      throw new ConnectException("Failed to parse claim check header JSON", e);
    }
  }

  private ClaimCheckValue parseFromMap(Map<?, ?> map) {
    try {
      return ClaimCheckValue.from(map);
    } catch (Exception e) {
      throw new ConnectException("Failed to parse claim check header map", e);
    }
  }

  @Override
  public ConfigDef config() {
    return Config.DEFINITION;
  }

  @Override
  public void close() {
    if (this.storage != null) {
      this.storage.close();
    }
  }
}
