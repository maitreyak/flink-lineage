package com.lineage;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Combined enrich + offset tracking function. Deserializes Avro, enriches with
 * Kafka metadata, tracks per-partition offset ranges in-memory, and writes
 * Parquet index files directly on checkpoint completion.
 *
 * Replaces the previous EnrichFunction + OffsetIndexFunction + second FileSink.
 */
public class OffsetTrackingEnrichFunction
        extends ProcessFunction<ConsumerRecord<byte[], byte[]>, GenericRecord>
        implements CheckpointedFunction, org.apache.flink.api.common.state.CheckpointListener {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OffsetTrackingEnrichFunction.class);

    // Accumulator array indices
    private static final int IDX_MIN_OFFSET = 0;
    private static final int IDX_MAX_OFFSET = 1;
    private static final int IDX_COUNT = 2;
    private static final int IDX_WINDOW_START = 3;

    private final String offsetIndexBasePath;

    // --- Transient (runtime) fields ---
    private transient GenericDatumReader<GenericRecord> reader;
    private transient DecoderFactory decoderFactory;

    /** In-memory accumulators: partition -> [minOffset, maxOffset, count, windowStart] */
    private transient HashMap<Integer, long[]> partitionAccumulators;

    /** Snapshots waiting for notifyCheckpointComplete */
    private transient ConcurrentHashMap<Long, HashMap<Integer, long[]>> pendingSnapshots;

    /** Flink managed list state for fault-tolerance */
    private transient ListState<byte[]> checkpointState;

    public OffsetTrackingEnrichFunction(String offsetIndexBasePath) {
        this.offsetIndexBasePath = offsetIndexBasePath;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        reader = new GenericDatumReader<>(AvroSchema.INPUT_EVENT_SCHEMA);
        decoderFactory = DecoderFactory.get();
        if (partitionAccumulators == null) {
            partitionAccumulators = new HashMap<>();
        }
        if (pendingSnapshots == null) {
            pendingSnapshots = new ConcurrentHashMap<>();
        }
    }

    // ---- ProcessFunction ----

    @Override
    public void processElement(
            ConsumerRecord<byte[], byte[]> record,
            Context ctx,
            Collector<GenericRecord> out) {
        try {
            // Deserialize Avro payload
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
                    .build();

            out.collect(enriched);

            // Update offset accumulators
            int partition = record.partition();
            long offset = record.offset();
            long[] acc = partitionAccumulators.get(partition);
            if (acc == null) {
                acc = new long[]{offset, offset, 1L, System.currentTimeMillis()};
                partitionAccumulators.put(partition, acc);
            } else {
                acc[IDX_MIN_OFFSET] = Math.min(acc[IDX_MIN_OFFSET], offset);
                acc[IDX_MAX_OFFSET] = Math.max(acc[IDX_MAX_OFFSET], offset);
                acc[IDX_COUNT]++;
            }
        } catch (IOException e) {
            LOG.error("Failed to deserialize Avro record from partition={} offset={}",
                    record.partition(), record.offset(), e);
        }
    }

    // ---- CheckpointedFunction ----

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        long checkpointId = context.getCheckpointId();

        // Serialize current accumulators
        checkpointState.clear();
        if (!partitionAccumulators.isEmpty()) {
            checkpointState.add(serializeAccumulators(partitionAccumulators));
        }

        // Stash a copy for notifyCheckpointComplete
        if (!partitionAccumulators.isEmpty()) {
            pendingSnapshots.put(checkpointId, deepCopy(partitionAccumulators));
        }

        // Reset for next checkpoint interval
        partitionAccumulators.clear();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<byte[]> descriptor =
                new ListStateDescriptor<>("offset-accumulators", TypeInformation.of(byte[].class));
        checkpointState = context.getOperatorStateStore().getListState(descriptor);

        partitionAccumulators = new HashMap<>();
        pendingSnapshots = new ConcurrentHashMap<>();

        if (context.isRestored()) {
            for (byte[] data : checkpointState.get()) {
                HashMap<Integer, long[]> restored = deserializeAccumulators(data);
                // Merge restored state
                for (Map.Entry<Integer, long[]> entry : restored.entrySet()) {
                    partitionAccumulators.merge(entry.getKey(), entry.getValue(), (existing, incoming) -> {
                        existing[IDX_MIN_OFFSET] = Math.min(existing[IDX_MIN_OFFSET], incoming[IDX_MIN_OFFSET]);
                        existing[IDX_MAX_OFFSET] = Math.max(existing[IDX_MAX_OFFSET], incoming[IDX_MAX_OFFSET]);
                        existing[IDX_COUNT] += incoming[IDX_COUNT];
                        existing[IDX_WINDOW_START] = Math.min(existing[IDX_WINDOW_START], incoming[IDX_WINDOW_START]);
                        return existing;
                    });
                }
            }
        }
    }

    // ---- CheckpointListener ----

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        HashMap<Integer, long[]> snapshot = pendingSnapshots.remove(checkpointId);
        if (snapshot == null || snapshot.isEmpty()) {
            return;
        }

        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        String path = String.format("%s/chk-%d/subtask-%d.parquet",
                offsetIndexBasePath, checkpointId, subtaskIndex);

        writeParquetIndex(path, snapshot);
        LOG.info("Wrote offset index: {} ({} partitions)", path, snapshot.size());
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        pendingSnapshots.remove(checkpointId);
    }

    // ---- Parquet writing ----

    private void writeParquetIndex(String path, HashMap<Integer, long[]> snapshot) throws IOException {
        // Use Flink's FileSystem (which has S3 plugin access) via a custom OutputFile
        FlinkOutputFile outputFile = new FlinkOutputFile(new Path(path));

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(outputFile)
                .withSchema(AvroSchema.OFFSET_INDEX_SCHEMA)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (Map.Entry<Integer, long[]> entry : snapshot.entrySet()) {
                int partition = entry.getKey();
                long[] acc = entry.getValue();

                GenericRecord record = new GenericRecordBuilder(AvroSchema.OFFSET_INDEX_SCHEMA)
                        .set("kafka_partition", partition)
                        .set("min_offset", acc[IDX_MIN_OFFSET])
                        .set("max_offset", acc[IDX_MAX_OFFSET])
                        .set("record_count", acc[IDX_COUNT])
                        .set("window_start", acc[IDX_WINDOW_START])
                        .build();

                writer.write(record);
            }
        }
    }

    /**
     * Adapts Flink's FileSystem (which can access S3 via the plugin) to Parquet's OutputFile API.
     */
    private static class FlinkOutputFile implements OutputFile {
        private final Path path;

        FlinkOutputFile(Path path) {
            this.path = path;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            FileSystem fs = path.getFileSystem();
            return new FlinkPositionOutputStream(fs.create(path, FileSystem.WriteMode.OVERWRITE));
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

    /**
     * Wraps Flink's FSDataOutputStream with position tracking for Parquet.
     */
    private static class FlinkPositionOutputStream extends PositionOutputStream {
        private final org.apache.flink.core.fs.FSDataOutputStream stream;
        private long position = 0;

        FlinkPositionOutputStream(org.apache.flink.core.fs.FSDataOutputStream stream) {
            this.stream = stream;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void write(int b) throws IOException {
            stream.write(b);
            position++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            stream.write(b, off, len);
            position += len;
        }

        @Override
        public void flush() throws IOException {
            stream.flush();
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }
    }

    // ---- Serialization helpers ----

    private static byte[] serializeAccumulators(HashMap<Integer, long[]> accumulators) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(accumulators.size());
        for (Map.Entry<Integer, long[]> entry : accumulators.entrySet()) {
            dos.writeInt(entry.getKey());
            long[] vals = entry.getValue();
            dos.writeLong(vals[IDX_MIN_OFFSET]);
            dos.writeLong(vals[IDX_MAX_OFFSET]);
            dos.writeLong(vals[IDX_COUNT]);
            dos.writeLong(vals[IDX_WINDOW_START]);
        }
        dos.flush();
        return baos.toByteArray();
    }

    private static HashMap<Integer, long[]> deserializeAccumulators(byte[] data) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
        int size = dis.readInt();
        HashMap<Integer, long[]> result = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            int partition = dis.readInt();
            long[] vals = new long[]{
                    dis.readLong(),  // minOffset
                    dis.readLong(),  // maxOffset
                    dis.readLong(),  // count
                    dis.readLong()   // windowStart
            };
            result.put(partition, vals);
        }
        return result;
    }

    private static HashMap<Integer, long[]> deepCopy(HashMap<Integer, long[]> source) {
        HashMap<Integer, long[]> copy = new HashMap<>(source.size());
        for (Map.Entry<Integer, long[]> entry : source.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().clone());
        }
        return copy;
    }
}
