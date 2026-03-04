package com.lineage;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Deserializes Avro messages from Kafka and enriches them with
 * Kafka metadata (topic, partition, offset, checkpoint_id).
 */
public class EnrichFunction
        extends ProcessFunction<ConsumerRecord<byte[], byte[]>, GenericRecord>
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EnrichFunction.class);

    public static final OutputTag<GenericRecord> DROPPED_TAG =
            new OutputTag<>("dropped", new GenericRecordAvroTypeInfo(AvroSchema.ENRICHED_EVENT_SCHEMA));

    private final String writeAheadCommitLogPath;

    private transient GenericDatumReader<GenericRecord> reader;
    private transient DecoderFactory decoderFactory;
    private transient long currentCheckpointId;
    private transient int subtaskIndex;
    private transient HashMap<Integer, Long> partitionMinOffset;
    private transient HashMap<Integer, Long> partitionMaxOffset;
    private transient HashMap<Integer, Long> partitionRecordCount;

    public EnrichFunction(String writeAheadCommitLogPath) {
        this.writeAheadCommitLogPath = writeAheadCommitLogPath;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        reader = new GenericDatumReader<>(AvroSchema.INPUT_EVENT_SCHEMA);
        decoderFactory = DecoderFactory.get();
        subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        partitionMinOffset = new HashMap<>();
        partitionMaxOffset = new HashMap<>();
        partitionRecordCount = new HashMap<>();
    }

    @Override
    public void processElement(
            ConsumerRecord<byte[], byte[]> record,
            Context ctx,
            Collector<GenericRecord> out) {
        // Track offset ranges before output/dropped split
        int partition = record.partition();
        long offset = record.offset();
        partitionMinOffset.merge(partition, offset, Math::min);
        partitionMaxOffset.merge(partition, offset, Math::max);
        partitionRecordCount.merge(partition, 1L, Long::sum);

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

            // Drop offsets divisible by 10 to test gap detection
            if (record.offset() % 10 != 0) {
                out.collect(enriched);
            } else {
                ctx.output(DROPPED_TAG, enriched);
            }
        } catch (IOException e) {
            LOG.error("Failed to deserialize Avro record from partition={} offset={}",
                    record.partition(), record.offset(), e);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        currentCheckpointId = context.getCheckpointId();

        if (!partitionMinOffset.isEmpty()) {
            writeOffsetRangesCsv(currentCheckpointId);
            partitionMinOffset.clear();
            partitionMaxOffset.clear();
            partitionRecordCount.clear();
        }
    }

    private void writeOffsetRangesCsv(long checkpointId) throws Exception {
        Instant instant = Instant.now();
        ZonedDateTime zdt = instant.atZone(ZoneOffset.UTC);
        String datePart = String.format("%02d-%02d-%d/%02d/%02d",
                zdt.getMonthValue(), zdt.getDayOfMonth(), zdt.getYear(),
                zdt.getHour(), zdt.getMinute());
        String filePath = String.format("%s/%s/offsets-subtask-%d-%s.csv",
                writeAheadCommitLogPath, datePart, subtaskIndex,
                UUID.randomUUID());

        StringBuilder csv = new StringBuilder();
        csv.append("checkpoint_id,kafka_partition,min_offset,max_offset,record_count\n");
        for (Map.Entry<Integer, Long> entry : partitionMinOffset.entrySet()) {
            int partition = entry.getKey();
            csv.append(checkpointId)
               .append(',')
               .append(partition)
               .append(',')
               .append(entry.getValue())
               .append(',')
               .append(partitionMaxOffset.get(partition))
               .append(',')
               .append(partitionRecordCount.get(partition))
               .append('\n');
        }

        Path fsPath = new Path(filePath);
        FileSystem fs = fsPath.getFileSystem();
        try (OutputStream out = fs.create(fsPath, FileSystem.WriteMode.OVERWRITE)) {
            out.write(csv.toString().getBytes(StandardCharsets.UTF_8));
        }
        LOG.info("Wrote offset ranges: checkpoint={} partitions={} path={}",
                checkpointId, partitionMinOffset.size(), filePath);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // No state to restore — checkpoint_id is transient, offset maps are rebuilt from replayed records
    }
}
