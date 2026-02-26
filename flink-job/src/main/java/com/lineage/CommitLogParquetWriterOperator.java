package com.lineage;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Operator that buffers commit log {@link GenericRecord}s per checkpoint and writes
 * them as Parquet files in {@code notifyCheckpointComplete()}.
 *
 * <p>Uses Flink's {@link FileSystem} API for S3 access (via the Flink S3 plugin),
 * wrapped in Parquet's {@link OutputFile} interface. No Hadoop S3A dependency needed.
 *
 * <p>Attached to the side output of {@link CommitLogExtractingOperator} via
 * {@code .transform()} to avoid the {@code ConcurrentModificationException} that
 * occurs when using {@code .sinkTo()} inside {@code addPreCommitTopology()}.
 */
public class CommitLogParquetWriterOperator
        extends AbstractStreamOperator<GenericRecord>
        implements OneInputStreamOperator<GenericRecord, GenericRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CommitLogParquetWriterOperator.class);

    private final String commitLogBasePath;

    /** Records buffered per checkpoint, written on checkpoint completion. */
    private transient ConcurrentHashMap<Long, List<GenericRecord>> pendingRecords;

    public CommitLogParquetWriterOperator(String commitLogBasePath) {
        this.commitLogBasePath = commitLogBasePath;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (pendingRecords == null) {
            pendingRecords = new ConcurrentHashMap<>();
        }
    }

    @Override
    public void processElement(StreamRecord<GenericRecord> element) throws Exception {
        GenericRecord record = element.getValue();
        long checkpointId = (long) record.get("checkpoint_id");
        pendingRecords.computeIfAbsent(checkpointId, k -> new ArrayList<>()).add(record);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);

        List<Long> completedIds = new ArrayList<>();
        for (Long cpId : pendingRecords.keySet()) {
            if (cpId <= checkpointId) {
                completedIds.add(cpId);
            }
        }

        for (Long cpId : completedIds) {
            List<GenericRecord> records = pendingRecords.remove(cpId);
            if (records != null && !records.isEmpty()) {
                writeParquet(cpId, records);
            }
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
        pendingRecords.remove(checkpointId);
    }

    private void writeParquet(long checkpointId, List<GenericRecord> records) throws IOException {
        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        String filePath = String.format("%s/chk-%d/subtask-%d.parquet",
                commitLogBasePath, checkpointId, subtaskIndex);

        OutputFile outputFile = new FlinkOutputFile(new Path(filePath));

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(AvroSchema.COMMIT_LOG_SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (GenericRecord record : records) {
                writer.write(record);
            }
        }

        LOG.info("Wrote commit log Parquet: {} ({} entries)", filePath, records.size());
    }

    /**
     * Adapts Flink's {@link FileSystem} to Parquet's {@link OutputFile} interface.
     * This allows ParquetWriter to write directly via Flink's S3 plugin.
     */
    private static class FlinkOutputFile implements OutputFile {
        private final Path path;

        FlinkOutputFile(Path path) {
            this.path = path;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            FileSystem fs = path.getFileSystem();
            OutputStream out = fs.create(path, FileSystem.WriteMode.OVERWRITE);
            return new FlinkPositionOutputStream(out);
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
            return 128 * 1024 * 1024; // 128 MB default
        }

        @Override
        public String getPath() {
            return path.toString();
        }
    }

    /**
     * Wraps a Flink {@link OutputStream} as a Parquet {@link PositionOutputStream},
     * tracking the current write position.
     */
    private static class FlinkPositionOutputStream extends PositionOutputStream {
        private final OutputStream delegate;
        private long pos = 0;

        FlinkPositionOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            pos++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            pos += len;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
