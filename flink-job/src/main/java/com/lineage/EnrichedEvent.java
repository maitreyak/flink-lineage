package com.lineage;

import java.io.Serializable;

/**
 * POJO enriched with Kafka metadata for lineage tracking.
 */
public class EnrichedEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String uuid;
    private long timestamp;
    private String kafkaTopic;
    private int kafkaPartition;
    private long kafkaOffset;

    public EnrichedEvent() {}

    public EnrichedEvent(String uuid, long timestamp, String kafkaTopic, int kafkaPartition, long kafkaOffset) {
        this.uuid = uuid;
        this.timestamp = timestamp;
        this.kafkaTopic = kafkaTopic;
        this.kafkaPartition = kafkaPartition;
        this.kafkaOffset = kafkaOffset;
    }

    public String getUuid() { return uuid; }
    public void setUuid(String uuid) { this.uuid = uuid; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public String getKafkaTopic() { return kafkaTopic; }
    public void setKafkaTopic(String kafkaTopic) { this.kafkaTopic = kafkaTopic; }

    public int getKafkaPartition() { return kafkaPartition; }
    public void setKafkaPartition(int kafkaPartition) { this.kafkaPartition = kafkaPartition; }

    public long getKafkaOffset() { return kafkaOffset; }
    public void setKafkaOffset(long kafkaOffset) { this.kafkaOffset = kafkaOffset; }

    @Override
    public String toString() {
        return "EnrichedEvent{uuid='" + uuid + "', timestamp=" + timestamp +
                ", kafkaTopic='" + kafkaTopic + "', kafkaPartition=" + kafkaPartition +
                ", kafkaOffset=" + kafkaOffset + "}";
    }
}
