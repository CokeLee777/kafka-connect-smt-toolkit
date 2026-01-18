package com.github.cokelee777.kafka.connect.smt.claimcheck.source;

import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.JsonConverterFactory;
import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.JsonRecordValueSerializer;
import com.github.cokelee777.kafka.connect.smt.claimcheck.internal.RecordValueSerializer;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckReference;
import com.github.cokelee777.kafka.connect.smt.claimcheck.model.ClaimCheckSchema;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageFactory;
import com.github.cokelee777.kafka.connect.smt.utils.ConfigUtils;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClaimCheckSourceTransform implements Transformation<SourceRecord> {

  private static final Logger log = LoggerFactory.getLogger(ClaimCheckSourceTransform.class);
  public static final String CONFIG_STORAGE_TYPE = "storage.type";
  public static final String CONFIG_THRESHOLD_BYTES = "threshold.bytes";
  private static final int DEFAULT_THRESHOLD_BYTES = 1024 * 1024;
  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              CONFIG_STORAGE_TYPE,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "Storage implementation type")
          .define(
              CONFIG_THRESHOLD_BYTES,
              ConfigDef.Type.INT,
              DEFAULT_THRESHOLD_BYTES,
              ConfigDef.Importance.HIGH,
              "Payload size threshold in bytes");

  private int thresholdBytes;
  private ClaimCheckStorage storage;
  private RecordValueSerializer recordValueSerializer;

  public int getThresholdBytes() {
    return thresholdBytes;
  }

  public ClaimCheckStorage getStorage() {
    return storage;
  }

  private static class TransformConfig extends AbstractConfig {
    TransformConfig(Map<String, ?> originals) {
      super(CONFIG_DEF, originals);
    }
  }

  public ClaimCheckSourceTransform() {}

  @Override
  public void configure(Map<String, ?> configs) {
    TransformConfig config = new TransformConfig(configs);

    this.thresholdBytes = config.getInt(CONFIG_THRESHOLD_BYTES);
    String storageType = ConfigUtils.getRequiredString(config, CONFIG_STORAGE_TYPE);

    this.storage = ClaimCheckStorageFactory.create(storageType);
    this.storage.configure(configs);

    JsonConverter converter = JsonConverterFactory.createValueConverter();
    this.recordValueSerializer = new JsonRecordValueSerializer(converter);

    log.info(
        "ClaimCheckTransform initialized. Threshold: {} bytes, Storage: {}",
        this.thresholdBytes,
        storageType);
  }

  @Override
  public SourceRecord apply(SourceRecord record) {
    if (record.value() == null) {
      return record;
    }

    byte[] serializedValue = this.recordValueSerializer.serialize(record);
    if (serializedValue == null) {
      return record;
    }

    if (serializedValue.length <= this.thresholdBytes) {
      return record;
    }

    String referenceUrl = this.storage.store(serializedValue);
    Struct referenceStruct =
        ClaimCheckReference.create(referenceUrl, serializedValue.length).toStruct();

    log.info(
        "Payload too large ({} bytes). Uploaded to storage: {}",
        serializedValue.length,
        referenceUrl);

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        ClaimCheckSchema.SCHEMA,
        referenceStruct,
        record.timestamp());
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    if (storage != null) {
      storage.close();
    }
  }
}
