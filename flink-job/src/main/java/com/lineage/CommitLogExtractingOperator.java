package com.lineage;

import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Operator inserted into the FileSink pre-commit topology that extracts
 * (checkpoint_id, s3_key) pairs from committable messages and writes
 * a JSON commit log file on checkpoint completion.
 *
 * All committable messages are forwarded unchanged — this operator is
 * purely an observer that doesn't affect the data path.
 */
public class CommitLogExtractingOperator
        extends AbstractStreamOperator<CommittableMessage<FileSinkCommittable>>
        implements OneInputStreamOperator<CommittableMessage<FileSinkCommittable>,
                                          CommittableMessage<FileSinkCommittable>> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CommitLogExtractingOperator.class);

    private final String commitLogBasePath;

    /** Accumulated entries for the current checkpoint interval: checkpoint_id -> list of s3 keys. */
    private transient HashMap<Long, List<String>> currentEntries;

    /** Snapshots waiting for notifyCheckpointComplete. */
    private transient ConcurrentHashMap<Long, List<CommitLogEntry>> pendingSnapshots;

    public CommitLogExtractingOperator(String commitLogBasePath) {
        this.commitLogBasePath = commitLogBasePath;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (currentEntries == null) {
            currentEntries = new HashMap<>();
        }
        if (pendingSnapshots == null) {
            pendingSnapshots = new ConcurrentHashMap<>();
        }
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<FileSinkCommittable>> element) throws Exception {
        CommittableMessage<FileSinkCommittable> message = element.getValue();

        // Extract commit log data from CommittableWithLineage messages
        if (message instanceof CommittableWithLineage) {
            CommittableWithLineage<FileSinkCommittable> cwl =
                    (CommittableWithLineage<FileSinkCommittable>) message;
            FileSinkCommittable committable = cwl.getCommittable();

            if (committable.hasPendingFile()) {
                InProgressFileWriter.PendingFileRecoverable pendingFile = committable.getPendingFile();
                Path path = pendingFile.getPath();
                if (path != null) {
                    long checkpointId = cwl.getCheckpointIdOrEOI();
                    String s3Key = path.toString();

                    currentEntries
                            .computeIfAbsent(checkpointId, k -> new ArrayList<>())
                            .add(s3Key);

                    LOG.debug("Commit log: checkpoint={} s3_key={}", checkpointId, s3Key);
                }
            }
        }

        // Forward unchanged
        output.collect(element);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        long checkpointId = context.getCheckpointId();
        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

        // Build snapshot entries from accumulated data
        if (!currentEntries.isEmpty()) {
            for (Map.Entry<Long, List<String>> entry : currentEntries.entrySet()) {
                long cpId = entry.getKey();
                List<String> keys = entry.getValue();
                long timestamp = System.currentTimeMillis();

                List<CommitLogEntry> entries = new ArrayList<>(keys.size());
                for (String s3Key : keys) {
                    entries.add(new CommitLogEntry(s3Key, timestamp));
                }
                // Merge into pending snapshots (a checkpoint may span multiple snapshot calls
                // if the same checkpoint ID appears, though typically it won't)
                pendingSnapshots.merge(cpId, entries, (existing, incoming) -> {
                    existing.addAll(incoming);
                    return existing;
                });
            }
            currentEntries.clear();
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        currentEntries = new HashMap<>();
        pendingSnapshots = new ConcurrentHashMap<>();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);

        List<CommitLogEntry> entries = pendingSnapshots.remove(checkpointId);
        if (entries == null || entries.isEmpty()) {
            return;
        }

        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        String path = String.format("%s/chk-%d/subtask-%d.json",
                commitLogBasePath, checkpointId, subtaskIndex);

        writeCommitLog(path, checkpointId, subtaskIndex, entries);
        LOG.info("Wrote commit log: {} ({} files)", path, entries.size());
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
        pendingSnapshots.remove(checkpointId);
    }

    private void writeCommitLog(String path, long checkpointId, int subtaskIndex,
                                List<CommitLogEntry> entries) throws IOException {
        Path fsPath = new Path(path);
        FileSystem fs = fsPath.getFileSystem();

        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"checkpoint_id\": ").append(checkpointId).append(",\n");
        json.append("  \"subtask_index\": ").append(subtaskIndex).append(",\n");
        json.append("  \"committed_files\": [\n");
        for (int i = 0; i < entries.size(); i++) {
            CommitLogEntry entry = entries.get(i);
            json.append("    {\"s3_key\": \"").append(escapeJson(entry.s3Key))
                .append("\", \"timestamp\": ").append(entry.timestamp).append("}");
            if (i < entries.size() - 1) {
                json.append(",");
            }
            json.append("\n");
        }
        json.append("  ]\n");
        json.append("}\n");

        try (OutputStream out = fs.create(fsPath, FileSystem.WriteMode.OVERWRITE)) {
            out.write(json.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String escapeJson(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /** Simple holder for a committed file entry. */
    private static class CommitLogEntry {
        final String s3Key;
        final long timestamp;

        CommitLogEntry(String s3Key, long timestamp) {
            this.s3Key = s3Key;
            this.timestamp = timestamp;
        }
    }
}
