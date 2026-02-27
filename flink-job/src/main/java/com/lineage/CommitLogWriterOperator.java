package com.lineage;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Operator that buffers commit log records per checkpoint and writes
 * them as CSV files in {@code notifyCheckpointComplete()}.
 *
 * <p>Attached to the side output of {@link CommitLogExtractingOperator} via
 * {@code .transform()} to avoid the {@code ConcurrentModificationException} that
 * occurs when using {@code .sinkTo()} inside {@code addPreCommitTopology()}.
 */
public class CommitLogWriterOperator
        extends AbstractStreamOperator<GenericRecord>
        implements OneInputStreamOperator<GenericRecord, GenericRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CommitLogWriterOperator.class);

    private final String commitLogBasePath;
    private final String filePrefix;

    /** Records buffered per checkpoint, written on checkpoint completion. */
    private transient ConcurrentHashMap<Long, List<GenericRecord>> pendingRecords;

    public CommitLogWriterOperator(String commitLogBasePath, String filePrefix) {
        this.commitLogBasePath = commitLogBasePath;
        this.filePrefix = filePrefix;
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
                writeCsv(cpId, records);
            }
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
        pendingRecords.remove(checkpointId);
    }

    private void writeCsv(long checkpointId, List<GenericRecord> records) throws IOException {
        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        String filePath = String.format("%s/chk-%d/%s-subtask-%d.csv",
                commitLogBasePath, checkpointId, filePrefix, subtaskIndex);

        StringBuilder csv = new StringBuilder();
        csv.append("checkpoint_id,s3_key,commit_timestamp\n");
        for (GenericRecord record : records) {
            csv.append(record.get("checkpoint_id"))
               .append(',')
               .append(record.get("s3_key"))
               .append(',')
               .append(record.get("commit_timestamp"))
               .append('\n');
        }

        Path fsPath = new Path(filePath);
        FileSystem fs = fsPath.getFileSystem();
        try (OutputStream out = fs.create(fsPath, FileSystem.WriteMode.OVERWRITE)) {
            out.write(csv.toString().getBytes(StandardCharsets.UTF_8));
        }

        LOG.info("Wrote commit log CSV: {} ({} entries)", filePath, records.size());
    }
}
