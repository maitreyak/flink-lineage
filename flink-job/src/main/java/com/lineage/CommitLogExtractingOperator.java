package com.lineage;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateless operator in the FileSink pre-commit topology that extracts
 * (checkpoint_id, s3_key) pairs from committable messages and emits them
 * as {@link GenericRecord} side outputs.
 *
 * The side output stream is captured downstream and written to a transactional
 * Parquet FileSink, ensuring commit log records participate in Flink's
 * two-phase commit and roll back on failure (no orphans).
 *
 * All committable messages are forwarded unchanged on the main output.
 */
public class CommitLogExtractingOperator
        extends AbstractStreamOperator<CommittableMessage<FileSinkCommittable>>
        implements OneInputStreamOperator<CommittableMessage<FileSinkCommittable>,
                                          CommittableMessage<FileSinkCommittable>> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CommitLogExtractingOperator.class);

    /** Side output tag for commit log records. */
    public static final OutputTag<GenericRecord> COMMIT_LOG_TAG =
            new OutputTag<>("commit-log", new GenericRecordAvroTypeInfo(AvroSchema.COMMIT_LOG_SCHEMA));

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

                    GenericRecord record = new GenericRecordBuilder(AvroSchema.COMMIT_LOG_SCHEMA)
                            .set("checkpoint_id", checkpointId)
                            .set("s3_key", s3Key)
                            .set("commit_timestamp", System.currentTimeMillis())
                            .build();

                    output.collect(COMMIT_LOG_TAG, new StreamRecord<>(record));
                    LOG.debug("Commit log side output: checkpoint={} s3_key={}", checkpointId, s3Key);
                }
            }
        }

        // Forward main committable message unchanged
        output.collect(element);
    }
}
