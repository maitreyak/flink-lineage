package com.lineage.producer;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/**
 * Serialization schema that round-robins records across all Kafka partitions,
 * ensuring even distribution regardless of producer parallelism.
 */
public class KeyedByteArraySerializationSchema implements KafkaRecordSerializationSchema<byte[]> {

    private static final long serialVersionUID = 1L;

    private final String topic;
    private transient long counter;

    public KeyedByteArraySerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(byte[] element, KafkaSinkContext context, Long timestamp) {
        int numPartitions = context.getPartitionsForTopic(topic).length;
        int partition = (int) (counter++ % numPartitions);
        return new ProducerRecord<>(topic, partition, null, element);
    }
}
