package com.lineage;

import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Operator in the FileSink pre-commit topology that extracts
 * (checkpoint_id, s3_key) pairs from committable messages and writes them
 * eagerly as CSV files to the write-ahead commit log path.
 *
 * <p>Writing happens in {@code processElement()}, making entries durable
 * before the checkpoint completes. No buffering, no state, no
 * {@code notifyCheckpointComplete} dependency.
 *
 * <p>Tradeoff: the write-ahead commit log may contain entries for failed
 * checkpoints. Consumers filter by checking if the referenced s3_key exists
 * (uncommitted files stay {@code .inprogress} and get cleaned up).
 *
 * <p>All committable messages are forwarded unchanged on the main output.
 */
public class WriteAheadCommitLogOperator
        extends AbstractStreamOperator<CommittableMessage<FileSinkCommittable>>
        implements OneInputStreamOperator<CommittableMessage<FileSinkCommittable>,
                                          CommittableMessage<FileSinkCommittable>> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WriteAheadCommitLogOperator.class);

    private final String commitLogBasePath;
    private final String filePrefix;

    private transient int subtaskIndex;

    public WriteAheadCommitLogOperator(String commitLogBasePath, String filePrefix) {
        this.commitLogBasePath = commitLogBasePath;
        this.filePrefix = filePrefix;
    }

    @Override
    public void open() throws Exception {
        super.open();
        subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<FileSinkCommittable>> element) throws Exception {
        CommittableMessage<FileSinkCommittable> message = element.getValue();

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
                    long commitTimestamp = System.currentTimeMillis();

                    writeCsv(checkpointId, s3Key, commitTimestamp);
                    LOG.info("Wrote write-ahead commit log: checkpoint={} s3_key={}", checkpointId, s3Key);
                }
            }
        }

        // Forward main committable message unchanged
        output.collect(element);
    }

    private void writeCsv(long checkpointId, String s3Key, long commitTimestamp) throws Exception {
        String filePath = String.format("%s/chk-%d/%s-subtask-%d.csv",
                commitLogBasePath, checkpointId, filePrefix, subtaskIndex);

        StringBuilder csv = new StringBuilder();
        csv.append("checkpoint_id,s3_key,commit_timestamp\n");
        csv.append(checkpointId)
           .append(',')
           .append(s3Key)
           .append(',')
           .append(commitTimestamp)
           .append('\n');

        Path fsPath = new Path(filePath);
        FileSystem fs = fsPath.getFileSystem();
        try (OutputStream out = fs.create(fsPath, FileSystem.WriteMode.OVERWRITE)) {
            out.write(csv.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
}
