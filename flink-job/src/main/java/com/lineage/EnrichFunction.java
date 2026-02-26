package com.lineage;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes Avro messages from Kafka and enriches them with
 * Kafka metadata (topic, partition, offset, checkpoint_id).
 */
public class EnrichFunction
        extends ProcessFunction<ConsumerRecord<byte[], byte[]>, GenericRecord>
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EnrichFunction.class);

    private transient GenericDatumReader<GenericRecord> reader;
    private transient DecoderFactory decoderFactory;
    private transient long currentCheckpointId;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        reader = new GenericDatumReader<>(AvroSchema.INPUT_EVENT_SCHEMA);
        decoderFactory = DecoderFactory.get();
    }

    @Override
    public void processElement(
            ConsumerRecord<byte[], byte[]> record,
            Context ctx,
            Collector<GenericRecord> out) {
        try {
            byte[] value = record.value();
            BinaryDecoder decoder = decoderFactory.binaryDecoder(value, null);
            GenericRecord inputRecord = reader.read(null, decoder);

            String uuid = inputRecord.get("uuid").toString();
            long timestamp = (Long) inputRecord.get("timestamp");

            GenericRecord enriched = new GenericRecordBuilder(AvroSchema.ENRICHED_EVENT_SCHEMA)
                    .set("uuid", uuid)
                    .set("timestamp", timestamp)
                    .set("kafka_topic", record.topic())
                    .set("kafka_partition", record.partition())
                    .set("kafka_offset", record.offset())
                    .set("checkpoint_id", currentCheckpointId)
                    .build();

            out.collect(enriched);
        } catch (IOException e) {
            LOG.error("Failed to deserialize Avro record from partition={} offset={}",
                    record.partition(), record.offset(), e);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        currentCheckpointId = context.getCheckpointId();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // No state to restore — checkpoint_id is transient
    }
}
